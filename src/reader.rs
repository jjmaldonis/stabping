/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

/*!
 * Structs and traits for reading back persistent data via `mmap` of an
 * associated target's data file.
 */
use std::mem;
use std::slice;
use std::io;
use std::io::{Write, BufWriter};
use std::sync::Arc;

use memmap::{Mmap, Protection};
use iron::response::{WriteBody};
use serde::{Serialize, Deserialize};

use crate::helpers::VecIntoRawBytes;
use crate::persist::TargetManager;
use crate::options::SENTINEL_NODATA;

/**
 * A request from the client for persistent data for a target in the time range
 * `lower` to `upper` in context of the target's current options, verified
 * with `nonce`.
 */
#[derive(Serialize, Deserialize, Debug)]
pub struct DataRequest {
    nonce: i32,
    lower: i32,
    upper: i32,
}

/**
 * Representation of data elements on-disk in a target's data file. They are
 * 32-bit back-to-back integers.
 */
#[repr(C, packed)]
struct DataElement {
    time: i32,
    index: i32,
    val: i32,
}

/**
 * A reader (implemented as an Iron body writer `WriteBody`) for the persistent
 * data of a target.
 */
pub struct SPDataReader {
    lower: i32,
    upper: i32,
    tm: Arc<TargetManager>,
}

impl SPDataReader {
    pub fn new(tm: Arc<TargetManager>, dr: DataRequest) -> Option<Self> {
        // verify the nonce, and refuse to create a reader if it doesn't match
        if dr.nonce != tm.options_read().nonce {
            return None;
        }

        Some(SPDataReader{
            lower: dr.lower,
            upper: dr.upper,
            tm: tm,
        })
    }
}

impl WriteBody for SPDataReader {
    /**
     * Writes the body of the response with the requested persistent data.
     */
    fn write_body(&mut self, res: &mut dyn io::Write) -> io::Result<()> {
        /*
         * acquire nonce and current indices (current state of addrs for this
         * target) from the TargetManager
         */
        let (nonce, ordered_list, mut membership): (i32, Vec<i32>, Vec<i32>) = self.tm.get_current_indices();

        // verify that the request nonce and the manager's nonce match
        if nonce != self.tm.options_read().nonce {
            println!("Nonce mismatch in WriteBody for SPDatReader!");
            return Ok(())
        }

        // get a lock on the target's data file
        let guard = self.tm.data_file_read();

        // attempt to mmap the target's data file
        let map =
            Mmap::open(&*guard, Protection::Read)
            .map_err(|e| {
                println!("ERROR: Mmap failed!");
                e
            })?;

        /*
         * attempt to read the raw bytes of the mapped data file as a series of
         * DataElements (three 32-bit integers back-to-back)
         */
        let data: &[DataElement] = unsafe {
            let orig = map.as_slice();
            let raw_ptr = orig.as_ptr();

            let orig_len = orig.len();
            if orig_len % mem::size_of::<DataElement>() != 0 {
                println!("ERROR: data file not a multiple 3 * 4 bytes!");
                return Err(io::Error::new(io::ErrorKind::Other, "Data file incorrect multiple!"));
            }
            let new_len = orig.len() / mem::size_of::<DataElement>();

            let _ = orig;
            slice::from_raw_parts(raw_ptr as *const DataElement, new_len)
        };

        // search for the requested start/lower/begin time of the data
        let begin = match data.binary_search_by_key(&self.lower, |d| d.time) {
            Ok(mut i) => {
                /*
                 * we may end up in the middle of a series of data points taken
                 * at the same time; we seek to the first
                 */
                while i > 0 && data[i].time == self.lower {
                    i -= 1;
                }
                i
            },
            Err(i) => i
        };

        // search for the requested end/upper time of the data
        let end = match data.binary_search_by_key(&self.upper, |d| d.time) {
            Ok(mut i) => {
                /*
                 * we may end up in the middle of a series of data points taken
                 * at the same time; we seek to the last
                 */
                while i < data.len() && data[i].time == self.upper {
                    i += 1;
                }
                i
            },
            Err(i) => i
        };

        /*
         * if our search reveals that we need to start past the data we have,
         * then we don't have that data
         */
        if begin >= data.len() {
            return Ok(())
        }

        // initialize a buffered writer to actually write the response body
        let mut writer = BufWriter::new(res);

        /*
         * we process the data in time-based segments, initialize a buffer of
         * the appropriate size to store that data until we write it
         */
        let mut buf: Vec<i32> = Vec::with_capacity(1 + ordered_list.len());
        let mut cur = data[begin].time;

        // loop through all the data points we have between begin and end
        for d in &data[begin..end] {
            /*
             * if we encounter a different time, process one complete time
             * segment and write it
             */
            if cur != d.time {
                // first element is time
                buf.push(cur);

                /*
                 * followed by data values in-order in which they appear in the
                 * target's current addrs (here tracked by the ordered_list
                 * of indices obtained from manager)
                 */
                for &i in ordered_list.iter() {
                    buf.push(membership[i as usize]);
                    membership[i as usize] = SENTINEL_NODATA;
                }

                // write out the data and reset our buffer and time tracker
                writer.write_all(&buf.into_raw_bytes())?;
                buf = Vec::with_capacity(1 + ordered_list.len());
                cur = d.time;
            }

            /*
             * if this data point is relevant to us, meaning the addr
             * represented by its index is in the target's current addrs (here
             * tracked by membership), then we store it (cheatingly in
             * membership indexed by its index -- this way we don't need to
             * allocate another buffer to store it)
             */
            if membership[d.index as usize] != 0 {
                membership[d.index as usize] = d.val;
            }
        }

        // process the final time segment, and flush our writer
        buf.push(cur);
        for &i in ordered_list.iter() {
            buf.push(membership[i as usize]);
            membership[i as usize] = SENTINEL_NODATA;
        }
        writer.write_all(&buf.into_raw_bytes())?;
        writer.flush()?;

        Ok(())
    }
}


