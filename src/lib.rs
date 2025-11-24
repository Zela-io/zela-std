use std::io::{Error as IoError, ErrorKind};

use serde::{Serialize, de::DeserializeOwned};
pub use serde_json::{self, Value as JsonValue, json};

use crate::rpc_client::ClientError;

pub mod rpc_client;
pub mod sys;

// this works because we don't provide any imports that are actually async
// we only provide an async interface for the user
//
// so we can rely on the fact that no future can actually wait for any IO asynchronously
// and the only progress it can make is by repeatedly calling .poll
fn poll_await<F: Future>(f: F) -> F::Output {
	let mut fut = std::pin::pin!(f);
	let mut ctx = std::task::Context::from_waker(std::task::Waker::noop());

	loop {
		match std::future::Future::poll(fut.as_mut(), &mut ctx) {
			// really this shouldn't happen (nothing to wait for, everything is still blocking),
			// but since we are dealing with user code we'll be lenient and skip past any `Pending`s
			std::task::Poll::Pending => (),
			std::task::Poll::Ready(r) => break r,
		}
	}
}

pub struct RpcError<E = JsonValue> {
	pub code: i32,
	pub message: String,
	pub data: Option<E>,
}

#[allow(async_fn_in_trait)] // 1. we are a consumer of the future, 2. we are in single-thread wasm
/// High-level trait for custom procedures.
pub trait CustomProcedure {
	type Params: DeserializeOwned;
	type SuccessData: Serialize;
	type ErrorData: Serialize;

	/// The entrypoint of the custom procedure.
	///
	/// This is called for each RPC of the custom procedure.
	///
	/// ## Async
	///
	/// Note that currently it is async only for interface reasons - there are no IO methods
	/// that actually execute concurrently exposed into the procedure runtime.
	/// However, for the sake of matching interface the of [`rpc_client::RpcClient`] and existing
	/// ecosystem code this is async so that it is possible to `.await` inside.
	///
	/// Note that the general advice of not blocking inside async doesn't apply here - blocking is the only way
	/// of making progress (wait for IO) right now.
	async fn run(params: Self::Params) -> Result<Self::SuccessData, RpcError<Self::ErrorData>>;

	/// Statically set maximum logging level.
	const LOG_MAX_LEVEL: log::LevelFilter = log::LevelFilter::Info;
	/// This function turns the log record produced by calls to `log::event!` macros into a string
	/// that will be sent to the host.
	fn log_fmt(record: &log::Record) -> String {
		format!("{} {}: {}", record.level(), record.target(), record.args())
	}
}

struct CustomProcedureLog {
	fmt: fn(&log::Record) -> String,
}
impl log::Log for CustomProcedureLog {
	fn enabled(&self, _metadata: &log::Metadata) -> bool {
		true
	}

	fn log(&self, record: &log::Record) {
		sys::rockawayx::zela::zela_host::log(&(self.fmt)(record))
	}

	fn flush(&self) {}
}

// we need another trait because we cannot put an associated constant onto `sys::Guest`
// and we cannot define an associated `static` either.
trait CustomProcedureInternal {
	const LOGGER: CustomProcedureLog;
	fn init();
}
impl<T: CustomProcedure> CustomProcedureInternal for T {
	const LOGGER: CustomProcedureLog = CustomProcedureLog { fmt: T::log_fmt };
	fn init() {
		log::set_logger(&Self::LOGGER).unwrap();
		log::set_max_level(T::LOG_MAX_LEVEL);
	}
}

// Auto-implement the bindings trait for the procedure if it has the high-level trait.
static INIT_CALLED: std::sync::Once = std::sync::Once::new();
impl<T: CustomProcedure> sys::Guest for T {
	fn run(params: sys::Json) -> Result<sys::Json, sys::RpcError> {
		INIT_CALLED.call_once(|| {
			<T as CustomProcedureInternal>::init();
		});

		let params: T::Params = match serde_json::from_slice(&params) {
			Ok(v) => v,
			Err(err) => {
				return Err(sys::rockawayx::zela::types::RpcError {
					code: 400, // TODO: define error codes
					message: format!("Failed to deserialize params: {err}"),
					data: None,
				});
			}
		};

		match poll_await(Self::run(params)) {
			Ok(v) => match serde_json::to_vec(&v) {
				Ok(v) => Ok(v),
				Err(err) => Err(sys::rockawayx::zela::types::RpcError {
					code: 500, // TODO: define error codes
					message: format!("Failed to serialize success data: {err}"),
					data: None,
				}),
			},
			Err(RpcError {
				code,
				message,
				data,
			}) => {
				let data = match data {
					None => None,
					Some(v) => match serde_json::to_vec(&v) {
						Ok(v) => Some(v),
						Err(err) => {
							return Err(sys::rockawayx::zela::types::RpcError {
								code: 500, // TODO: define error codes
								message: format!("Failed to serialize error data: {err}"),
								data: None,
							});
						}
					},
				};

				Err(sys::rockawayx::zela::types::RpcError {
					code,
					message,
					data,
				})
			}
		}
	}
}

/// Make an remote procedure call from the procedure.
pub fn call_rpc<P: Serialize, Rs: DeserializeOwned, Re: DeserializeOwned>(
	method: &str,
	params: P,
) -> Result<Result<Rs, RpcError<Re>>, IoError> {
	let params =
		serde_json::to_vec(&params).map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;
	match sys::rockawayx::zela::zela_host::call_rpc(method, &params) {
		Ok(Ok(v)) => {
			Ok(Ok(serde_json::from_slice(&v).map_err(|err| {
				IoError::new(ErrorKind::InvalidInput, err)
			})?))
		}
		Ok(Err(sys::rockawayx::zela::types::RpcError {
			code,
			message,
			data,
		})) => Ok(Err(RpcError {
			code,
			message,
			data: match data {
				None => None,
				Some(v) => Some(
					serde_json::from_slice(&v)
						.map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?,
				),
			},
		})),
		Err(err) => Err(IoError::from_raw_os_error(err as _)),
	}
}

pub struct Subscription(sys::rockawayx::zela::zela_host::RpcSubscription);
impl Subscription {
	pub fn new<P: Serialize, Re: DeserializeOwned>(
		method: &str,
		params: P,
	) -> Result<Result<Self, RpcError<Re>>, IoError> {
		let params = serde_json::to_vec(&params)
			.map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?;
		match sys::rockawayx::zela::zela_host::RpcSubscription::subscribe(method, &params) {
			Ok(Ok(v)) => Ok(Ok(Self(v))),
			Ok(Err(sys::rockawayx::zela::types::RpcError {
				code,
				message,
				data,
			})) => Ok(Err(RpcError {
				code,
				message,
				data: match data {
					None => None,
					Some(v) => Some(
						serde_json::from_slice(&v)
							.map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?,
					),
				},
			})),
			Err(err) => Err(IoError::from_raw_os_error(err as _)),
		}
	}

	pub fn recv<N: DeserializeOwned>(&self) -> Result<N, IoError> {
		match self.0.recv() {
			Ok(v) => Ok(serde_json::from_slice(&v)
				.map_err(|err| IoError::new(ErrorKind::InvalidInput, err))?),
			Err(err) => Err(IoError::from_raw_os_error(err as _)),
		}
	}
}

impl<T> From<ClientError> for RpcError<T> {
	fn from(value: ClientError) -> Self {
		RpcError {
			code: 500,
			message: value.to_string(),
			data: None,
		}
	}
}

#[macro_export]
macro_rules! zela_custom_procedure {
	( $name: ident ) => {
		$crate::sys::export!($name with_types_in $crate::sys);
	};
}
