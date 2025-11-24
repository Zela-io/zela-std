#![allow(clippy::collapsible_if)]
#![allow(mismatched_lifetime_syntaxes)]

mod client;
mod config;
mod error;
mod filter;
mod pubsub;
mod request;
pub mod response;
mod util;

pub use client::*;
pub use config::*;
pub use error::{Error as ClientError, ErrorKind as ClientErrorKind, Result as ClientResult};
pub use pubsub::{PubsubClient, PubsubClientError};
pub use request::RpcRequest;
use request::*;
use response::*;
use util::*;

// we return a struct implementing Stream from PubsubClient
pub use futures_util::{Stream, StreamExt};
