/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

use std::thread;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;

use std::time::{Duration, Instant};
use time::precise_time_ns;
use chrono::Local;

use std::net::{TcpStream, ToSocketAddrs};

use crate::options::SENTINEL_ERROR;
use crate::options::TargetResults;
use crate::persist::TargetManager;

/**
 * Runs the TCP Ping target's data-collection worker.
 */
pub fn run_tcpping_worker(manager: Arc<TargetManager>,
                          results_out: Sender<TargetResults>) -> thread::JoinHandle<()> {
    // start a new thread for the worker
    thread::spawn(move || {
        let mut handles = Vec::new();

        // continue to collect data forever
        loop {
            let loop_start = Instant::now();

            // retrieve the target's current options
            let (dur_interval, avg_across, dur_pause, num_addrs) = {
                let ref opt = manager.options_read();
                (
                    Duration::from_millis(opt.interval as u64),
                    opt.avg_across,
                    Duration::from_millis(opt.pause as u64),
                    opt.addrs.len(),
                )
            };

            // get the current time (to timestamp this round of data with)
            let timestamp: i32 = Local::now().timestamp() as i32;

            let nonce = {
                let ref t_opt = manager.options_read();
                for addr in t_opt.addrs.iter() {
                    let a = addr.clone();

                    /*
                     * create channels so the per-addr threads can send back
                     * their data to the worker thread
                     */
                    let (tx, rx) = channel();
                    handles.push(rx);

                    /*
                     * spawn a thread to actually collect the data for each
                     * separate address
                     */
                    thread::spawn(move || {
                        let mut sum = 0;
                        let mut denom = 0;
                        // average the results across the given number of times
                        for _ in 0..avg_across {
                            /*
                             * time the duration of a TCP handshake to the
                             * address
                             */
                            let start = precise_time_ns();
                            // Set a 30 second timeout for the TCP connection
                            let timeout = Duration::from_secs(30);
                            if let Ok(mut addrs) = a.as_str().to_socket_addrs() {
                                if let Some(addr) = addrs.next() {
                                    if TcpStream::connect_timeout(&addr, timeout).is_ok() {
                                        sum += precise_time_ns() - start;
                                        denom += 1;
                                    }
                                }
                            }
                            thread::sleep(dur_pause);
                        }

                        if denom != 0 {
                            /*
                             * send back micro-second average.
                             *
                             * we don't care if send fails as that likely means
                             * we took too long and the control thread is no longer
                             * waiting for us
                             */
                            let _ = tx.send((sum / denom / 1000) as i32);
                        }
                    });
                }
                t_opt.nonce
            };

            let mut data: Vec<i32> = Vec::with_capacity(3 + num_addrs);

            data.push(manager.kind.kind_id());
            data.push(nonce);
            data.push(timestamp);

            // read back the data from the per-addr subthreads, blocking
            // until each one completes (they always terminate due to the
            // TCP connect timeout)
            for h in handles.drain(..) {
                if let Ok(val) = h.recv() {
                    data.push(val);
                } else {
                    // all sub-attempts failed (sender dropped without sending)
                    data.push(SENTINEL_ERROR);
                }
            }

            // send off our results to the main thread
            if results_out.send(TargetResults(data)).is_err() {
                println!("Worker Control: failed to send final results back.");
            }

            // sleep for the remainder of the interval
            let elapsed = loop_start.elapsed();
            if elapsed < dur_interval {
                thread::sleep(dur_interval - elapsed);
            }
        }
    })
}

