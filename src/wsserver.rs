/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

use std::thread;
use std::sync::{Arc, Mutex, RwLock};

use ws;
use ws::{Settings, Builder};

use crate::options::MainConfiguration;

/**
 * Error container for websocket broadcasts.
 */
pub enum BroadcastError {
    SocketNotAvail,
    WebSocketError(ws::Error)
}

/**
 * Wrapper around a websocket broadcast sender for sharing across threads.
 */
pub struct Broadcaster {
    sender: Mutex<Option<ws::Sender>>,
}

impl Broadcaster {
    pub fn new() -> Broadcaster {
        Broadcaster {
            sender: Mutex::new(None),
        }
    }

    /**
     * Sets/updates the broadcast sender wrapped by this wrapper.
     */
    fn update(&self, new_sender: ws::Sender) {
        let mut guard = self.sender.lock().unwrap();
        *guard = Some(new_sender);
    }

    /**
     * Broadcasts (sends through the broadcast sender) a message to all
     * connected websocket clients.
     */
    pub fn send<M>(&self, msg: M) -> Result<(), BroadcastError> where M: Into<ws::Message> {
        let guard = self.sender.lock().unwrap();
        if let Some(ref b) = *guard {
            b.send(msg).map_err(|e| BroadcastError::WebSocketError(e))
        } else {
            Err(BroadcastError::SocketNotAvail)
        }
    }
}

pub fn ws_server(configuration: Arc<RwLock<MainConfiguration>>,
                 broadcaster: Arc<Broadcaster>) -> thread::JoinHandle<()> {
    let ws_port = configuration.read().unwrap().ws_port;
    thread::spawn(move || {
        loop {
            let socket = {
                let mut builder = Builder::new();
                builder.with_settings(Settings::default());
                /*
                 * build a socket with a dumb factory and handler, as we only
                 * need the socket for the broadcaster
                 */
                builder.build(|_| {
                    move |_| {
                        Ok(())
                    }
                }).unwrap()
            };
            broadcaster.update(socket.broadcaster());
            println!("WebSocket server (re)listening on port {}.", ws_port);
            socket.listen(("0.0.0.0", ws_port))
                  .expect("Unable to listen on websocket.");
        }
    })
}
