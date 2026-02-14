/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

use std::path::Path;
use std::thread;
use std::sync::Arc;
use std::sync::mpsc::Sender;

use crate::persist::{TargetManager, ManagerError};
use crate::tcpping::run_tcpping_worker;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct TargetOptions {
    pub nonce: i32,
    pub addrs: Vec<String>,  // Vec of addresses (IPs to hit with TCP, files to download, etc.)
    pub interval: u32,  // interval between collection attempts, in millis
    pub avg_across: u32,  // number of sub-attempts average across for each interval
    pub pause: u32,  // pause between sub-attempts, in millis
}

pub static SENTINEL_ERROR: i32 = -2_100_000_000;
pub static SENTINEL_NODATA: i32 = -2_000_000_000;

/*
 * Data for each address. Structured as:
 * [kind, nonce, timestamp, datapoint1, datapoint2, ...]
 *
 * where kind is the kind_id for the TargetKind this result is coming from
 * where nonce determines the state of TargetOptions when these data were collected
 * where timestamp is in seconds from epoch,
 *
 * each datapoint is for each address in TargetOptions.addrs
 * (encoding of data inside the i32 is target-defined, or one of the
 * sentinel values for error or nodata),
 */
pub struct TargetResults(pub Vec<i32>);

pub enum TargetKind {
    TcpPing,
    HttpDownload,
}

static ALL_KINDS: [TargetKind; 1] = [TargetKind::TcpPing];

impl TargetKind {
    pub fn kind_id(&self) -> i32 {
        match *self {
            TargetKind::TcpPing => 0,
            TargetKind::HttpDownload => 1
        }
    }

    pub fn compact_name(&self) -> &'static str {
        match *self {
            TargetKind::TcpPing => "tcpping",
            TargetKind::HttpDownload => "httpdownload",
        }
    }

    pub fn default_options(&self) -> TargetOptions {
        match *self {
            TargetKind::TcpPing => TargetOptions {
                nonce: 0,
                addrs: vec!["google.com:80".to_owned(), "8.8.8.8:53".to_owned()],
                interval: 10_000,
                avg_across: 3,
                pause: 100,
            },
            _ => unimplemented!()
        }
    }

    pub fn run_worker(&self, manager: Arc<TargetManager>,
                             results_out: Sender<TargetResults>) -> thread::JoinHandle<()> {
        match *self {
            TargetKind::TcpPing => run_tcpping_worker(manager, results_out),
            _ => unimplemented!()
        }
    }

    pub fn new_managers_for_all<'a>(data_path: &'a Path) -> Result<Vec<Arc<TargetManager>>, ManagerError> {
        let mut targets = Vec::with_capacity(ALL_KINDS.len());
        for k in ALL_KINDS.iter() {
            targets.push(
                Arc::new(TargetManager::new(k, data_path)?)
            );
        }
        Ok(targets)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MainConfiguration {
    pub web_port: u16,
    pub ws_port: u16,
}

impl Default for MainConfiguration {
    fn default() -> Self {
        MainConfiguration {
            web_port: 5001,
            ws_port: 5002,
        }
    }
}

#[test]
fn ensure_kind_id_and_all_kinds_order_match() {
    for (i, k) in ALL_KINDS.iter().enumerate() {
        assert!(i as i32 == k.kind_id());
    }
}
