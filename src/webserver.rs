/*
 * Copyright 2016 icasdri
 *
 * This file is part of stabping. The original source code for stabping can be
 * found at <https://github.com/icasdri/stabping>. See COPYING for licensing
 * details.
 */

use std::thread;
use std::error::Error;
use std::fmt;
use std::io::Read;
use std::sync::Arc;
use std::sync::RwLock;

use iron::prelude::{Request, Response, Iron, IronResult, IronError};
use iron::middleware::Handler;
use iron::method::Method;
use iron::headers::ContentType;
use iron::modifiers::Header;
use iron::request::Body;
use iron::status;
use router::Router;
use mount::Mount;
use serde::de::DeserializeOwned;

use crate::reader::{SPDataReader, DataRequest};
use crate::persist::TargetManager;
use crate::options::{MainConfiguration, TargetOptions};

/**
 * Stabping-specific web error container for use in Iron web responses.
 */
#[derive(Debug)]
enum SPWebError {
    NotFound,
    InvalidMethod,
    NotImplemented,
    BadRequest,
    ServerError,
    NonceConflict,
}

impl Error for SPWebError {
    fn description(&self) -> &str {
        match *self {
            SPWebError::NotFound => "Resource not found.",
            SPWebError::InvalidMethod => "Invalid method.",
            SPWebError::NotImplemented => "Handler not yet implemented.",
            SPWebError::BadRequest => "Bad request (malformed or missing fields).",
            SPWebError::ServerError => "Server encountered an error.",
            SPWebError::NonceConflict => "The nonce given does not match the current nonce, refusing update.",
        }
    }
}

impl fmt::Display for SPWebError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            SPWebError::NotFound => "Resource not found.",
            SPWebError::InvalidMethod => "Invalid method.",
            SPWebError::NotImplemented => "Handler not yet implemented.",
            SPWebError::BadRequest => "Bad request (malformed or missing fields).",
            SPWebError::ServerError => "Server encountered an error.",
            SPWebError::NonceConflict => "The nonce given does not match the current nonce, refusing update.",
        })
    }
}


/**
 * A container for compiled-in binary and text web assets that are to be
 * statically served at the /assets endpoint.
 */
enum WebAssetContainer {
    Binary(&'static [u8]),
    Text(&'static str)
}

/*
 * Include (at compile-time) the web assets discovered during the build
 * process. See build.rs for details.
 */
include!(concat!(env!("OUT_DIR"), "/webassets_handler_body.rs"));

/**
 * A handler for the /assets endpoint to serve up the compiled-in assets.
 */
fn webassets_handler(req: &mut Request) -> IronResult<Response> {
    // serve the appropriate path, or index.html if none specified
    let path = {
        let p = req.url.path()[0];
        if p.len() > 0 {
            p
        } else {
            "index.html"
        }
    };

    match _webassets_handler_body(path) {
        Some((asset, ct)) => {
            let s = status::Ok;
            let h = Header(ContentType(ct.parse().unwrap()));
            Ok(match asset {
                WebAssetContainer::Binary(b) => Response::with((s, h, b)),
                WebAssetContainer::Text(t) => Response::with((s, h, t)),
            })
        },
        None => Err(IronError::new(SPWebError::NotFound, status::NotFound))
    }
}

/**
 * Helper trait for reading JSON from HTTP request bodies (as in e.g. POST
 * requests).
 */
trait JsonBody {
    fn read_json<T: DeserializeOwned>(&mut self) -> Result<T, IronError>;
}

impl<'a, 'b> JsonBody for Body<'a, 'b> {
    fn read_json<T: DeserializeOwned>(&mut self) -> Result<T, IronError> {
        let mut buf = String::new();

        self.read_to_string(&mut buf)
            .map_err(|_| {
                println!("Failed to read request body.");
                IronError::new(SPWebError::ServerError, status::InternalServerError)
            })?;

        serde_json::from_str::<T>(&buf)
            .map_err(|_| {
                println!("Failed to parse request body.");
                IronError::new(SPWebError::BadRequest, status::BadRequest)
            })
    }
}

/**
 * Handler for each /api/target endpoint that handles returning and updating
 * target options, and retrieving persisted target data.
 */
struct TargetHandler {
    manager: Arc<TargetManager>,
}

impl TargetHandler {
    fn new(manager: Arc<TargetManager>) -> Self {
        TargetHandler {
            manager: manager,
        }
    }
}

impl Handler for TargetHandler {
    fn handle(&self, req: &mut Request) -> IronResult<Response> {
        match req.method {
            Method::Get => { /* Get Options */
                println!("Request for {} options.", self.manager.kind.compact_name());
                let options_ser = {
                    let options_guard = self.manager.options_read();
                    serde_json::to_string(&*options_guard).unwrap()
                };
                Ok(Response::with((status::Ok, options_ser)))
            },
            Method::Post => { /* Retrieve Data */
                // try and get the parameters of the request
                let dr: DataRequest = req.body.read_json()?;
                println!("Request for {} data: {:?}", self.manager.kind.compact_name(), dr);

                let body_writer =
                    // try and create a data reader out of this request
                    SPDataReader::new(self.manager.clone(), dr)
                    .ok_or_else(|| {
                        println!("Failed to create SPDataReader.");
                        IronError::new(SPWebError::BadRequest, status::BadRequest)
                    })?;

                // respond with the data reader (aka. body writer) as the body
                let r = Response::with(status::Ok);
                Ok(Response {
                    status: r.status,
                    headers: r.headers,
                    extensions: r.extensions,
                    body: Some(Box::new(body_writer)),
                })
            },
            Method::Put => { /* Update Options */
                // try and get the new/updated options from the request
                let mut new_options: TargetOptions = req.body.read_json()?;

                // make sure the received nonce matches the existing nonce
                if new_options.nonce != self.manager.options_read().nonce {
                    return Err(IronError::new(SPWebError::NonceConflict, status::Conflict));
                }

                // increment (and wrap-around if necessary) the nonce
                let new_nonce = {
                    let (n, over) = new_options.nonce.overflowing_add(1);
                    if over {
                        0
                    } else {
                        n
                    }
                };
                new_options.nonce = new_nonce;

                // actually update the options via the manager
                self.manager.options_update(new_options)
                    .map_err(|_| IronError::new(SPWebError::ServerError, status::InternalServerError))?;
                Ok(Response::with((format!("{}", new_nonce), status::Ok)))
            },
            _ => Err(IronError::new(SPWebError::InvalidMethod, status::MethodNotAllowed))
        }
    }
}


/**
 * Creates and starts the web server given the configuration (with the web
 * port) and a list of target managers.
 */
pub fn web_server<'a, T>(configuration: Arc<RwLock<MainConfiguration>>,
                         targets: T) -> thread::JoinHandle<()>
                         where T: Iterator<Item=&'a Arc<TargetManager>> {
    let mut router = Router::new();

    // serve index.html at root
    router.get("/", webassets_handler, "index");

    /*
     * serve the websockets port at /api/config/ws_port so that clients know
     * how to connect to the websockets server
     */
    let ws_port_str = format!("{}", configuration.read().unwrap().ws_port);
    let ws_port_handler = move |_: &mut Request| -> IronResult<Response> {
        Ok(Response::with((status::Ok, ws_port_str.as_str())))
    };
    router.get("/api/config/ws_port", ws_port_handler, "api_config_ws_port");

    // route each /api/target/... endpoint to the appropriate TargetHandler
    for tm in targets {
        router.any(format!("/api/target/{}", tm.kind.compact_name()),
                   TargetHandler::new(tm.clone()),
                   format!("target_{}", tm.kind.compact_name()));
    }

    let mut mount = Mount::new();
    mount.mount("/", router);

    // serve the web assets (via the web assets handler) at /assets
    mount.mount("/assets/", webassets_handler);

    let iron = Iron::new(mount);

    // actually spawn the Iron web server in a new thread
    let web_port = configuration.read().unwrap().web_port;
    thread::spawn(move || {
        println!("Web server listening on port {}.", web_port);
        iron.http(("0.0.0.0", web_port)).unwrap();
    })
}
