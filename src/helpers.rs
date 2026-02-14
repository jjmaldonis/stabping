/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

/*!
 * Helper traits and functions for reducing verbosity, wraping errors, and
 * containing unsafe code for many commonly used I/O and parsing operations.
 */
use std::mem;
use std::fmt::{self, Display};
use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use std::io::{Read, Write};

use serde::{Serialize, de::DeserializeOwned};

/**
 * Stabping-specific I/O error container, representing the possible failrue
 * cases when working with files, and wrapping an optional path (if one is
 * known).
 */
#[derive(Debug)]
pub enum SPIOError {
    Open(Option<PathBuf>),
    Read(Option<PathBuf>),
    Metadata(Option<PathBuf>),
    Write(Option<PathBuf>),
    Parse(Option<PathBuf>),
}

impl SPIOError {
    pub fn description(&self) -> String {
        let (verb, maybe_path) = match *self {
            SPIOError::Open(ref p) => ("open", p),
            SPIOError::Read(ref p) => ("read", p),
            SPIOError::Metadata(ref p) => ("get metadata", p),
            SPIOError::Write(ref p) => ("write", p),
            SPIOError::Parse(ref p) => ("parse", p),
        };

        let path_str = match maybe_path {
            &Some(ref path) => match path.to_str() {
                Some(s) => s,
                None => "",
            },
            &None => "",
        };

        format!("Unable to {} '{}'", verb, path_str)
    }
}

impl Display for SPIOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.description())
    }
}


/**
 * Trait for turning arbitrary data into a series of bytes that can be put directly
 * into a file or onto the network.
 */
pub trait VecIntoRawBytes {
    fn into_raw_bytes(self) -> Vec<u8>;
}

impl VecIntoRawBytes for Vec<i32> {
    fn into_raw_bytes(mut self) -> Vec<u8> {
        let raw_ptr = self.as_mut_ptr();
        let new_len = self.len() * mem::size_of::<i32>();
        let new_cap = self.capacity() * mem::size_of::<i32>();

        unsafe {
            // take full control over memory originally controlled by orig_data
            mem::forget(self);
            Vec::from_raw_parts(raw_ptr as *mut u8, new_len, new_cap)
        }
    }
}

/**
 * Expands the functionality of `File` to include JSON encoding, a
 * generalized `open()` and streamlined access to `metadata.length`. All
 * methods return a `Result<_, SPIOError>`, the `Result` wrapping the error
 * container from this module.
 *
 * Methods come in "basic" and `_p`/"with optional path" form. They accomplish
 * the same thing, except one takes an optional path that will be wrapped in
 * the error container if an error is encountered.
 */
pub trait SPFile {
    /**
     * Opens a file from the given path with the given `OpenOptions`.
     */
    fn open_from<'a, 'b>(oo: &'b mut OpenOptions, path: &'a Path) -> Result<File, SPIOError>;

    /**
     * Attempts to read from this file and decode all its contents as a JSON
     * object (serde::Deserialize).
     */
    fn read_json<T: DeserializeOwned>(&mut self) -> Result<T, SPIOError> {
        self._read_json(None)
    }
    fn read_json_p<'a, T: DeserializeOwned>(&mut self, path: &'a Path) -> Result<T, SPIOError> {
        self._read_json(Some(path))
    }
    fn _read_json<'a, T: DeserializeOwned>(&mut self, path: Option<&'a Path>) -> Result<T, SPIOError>;


    /**
     * Attempts to write a JSON object (serde::Serialize) to this file.
     */
    fn write_json<'b, T: Serialize>(&mut self, obj: &'b T) -> Result<(), SPIOError> {
        self._write_json(obj, None)
    }
    fn write_json_p<'a, 'b, T: Serialize>(&mut self, obj: &'b T, path: &'a Path) -> Result<(), SPIOError> {
        self._write_json(obj, Some(path))
    }
    fn _write_json<'a, 'b, T: Serialize>(&mut self, obj: &'b T, path: Option<&'a Path>) -> Result<(), SPIOError>;


    /**
     * Attempts to obtain the length of this file from filesystem metadata.
     */
    fn length(&mut self) -> Result<u64, SPIOError> {
        self._length(None)
    }
    fn length_p<'a>(&mut self, path: &'a Path) -> Result<u64, SPIOError> {
        self._length(Some(path))
    }
    fn _length<'a>(&mut self, path: Option<&'a Path>) -> Result<u64, SPIOError>;
}

impl SPFile for File {
    fn open_from<'a, 'b>(oo: &'b mut OpenOptions, path: &'a Path) -> Result<File, SPIOError> {
        Ok(
            oo.open(path)
            .map_err(|_| SPIOError::Open(Some(path.to_owned())))?
        )
    }

    fn _read_json<'a, T: DeserializeOwned>(&mut self, path: Option<&'a Path>) -> Result<T, SPIOError> {
        let mut buffer = String::new();
        self.read_to_string(&mut buffer)
            .map_err(|_| SPIOError::Read(path.map(|p| p.to_owned())))?;
        serde_json::from_str::<T>(&buffer)
            .map_err(|_| SPIOError::Parse(path.map(|p| p.to_owned())))
    }

    fn _write_json<'a, 'b, T: Serialize>(&mut self, obj: &'b T, path: Option<&'a Path>) -> Result<(), SPIOError> {
        let buffer = serde_json::to_string(obj).unwrap();
        self.write_all(buffer.as_bytes())
            .map_err(|_| SPIOError::Write(path.map(|p| p.to_owned())))?;
        self.flush()
            .map_err(|_| SPIOError::Write(path.map(|p| p.to_owned())))?;
        Ok(())
    }

    fn _length<'a>(&mut self, path: Option<&'a Path>) -> Result<u64, SPIOError> {
        let meta =
            self.metadata()
            .map_err(|_| SPIOError::Metadata(path.map(|p| p.to_owned())))?;
        Ok(meta.len())
    }
}

/**
 * Overwrite (create if necessary, truncate if already exists) the file
 * residing at the given path with the given JSON object (serde::Serialize).
 */
pub fn overwrite_json<'a, 'b, T: Serialize>(obj: &'a T, path: &'b Path) -> Result<(), SPIOError> {
    let mut file =
        OpenOptions::new().write(true).truncate(true).create(true).open(path)
        .map_err(|_| SPIOError::Open(Some(path.to_owned())))?;

    file.write_json_p(obj, path)?;
    Ok(())
}
