//! This Pubsub Client is based on and should be interface compatible with <https://crates.io/crates/solana-pubsub-client>.

use std::pin::Pin;

use serde_json::{Value, json};
use solana_account_decoder_client_types::UiAccount;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};

use super::{config::*, response::Response as RpcResponse, response::*};

#[derive(Debug, thiserror::Error)]
pub enum PubsubClientError {
	#[error("subscribe failed: {code} {message}")]
	SubscribeFailed {
		code: i32,
		message: String,
		data: Option<Value>,
	},
	#[error(transparent)]
	Io(#[from] std::io::Error),
}

type SubscribeResult<T> = Result<PubsubClientStream<T>, PubsubClientError>;

pub struct PubsubClientStream<T: serde::de::DeserializeOwned> {
	subscription: crate::Subscription,
	_boo: std::marker::PhantomData<T>,
}
impl<T: serde::de::DeserializeOwned> futures_util::Stream for PubsubClientStream<T> {
	type Item = T;

	fn poll_next(
		self: Pin<&mut Self>,
		_cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Option<Self::Item>> {
		std::task::Poll::Ready(match self.subscription.recv() {
			Ok(v) => Some(v),
			Err(err) => {
				log::trace!("Failed to receive a subscription notification: {err}");
				None
			}
		})
	}
}

pub struct PubsubClient {
	_priv: (),
}
impl PubsubClient {
	#[allow(clippy::new_without_default)]
	pub fn new() -> Self {
		Self { _priv: () }
	}

	async fn subscribe<T: serde::de::DeserializeOwned>(
		&self,
		method: &str,
		params: Value,
	) -> SubscribeResult<T> {
		let subscription = match crate::Subscription::new::<Value, Value>(method, params) {
			Ok(Ok(v)) => v,
			Ok(Err(crate::RpcError {
				code,
				message,
				data,
			})) => {
				return Err(PubsubClientError::SubscribeFailed {
					code,
					message,
					data,
				});
			}
			Err(err) => return Err(PubsubClientError::Io(err)),
		};

		Ok(PubsubClientStream {
			subscription,
			_boo: std::marker::PhantomData,
		})
	}

	pub async fn account_subscribe(
		&self,
		pubkey: &Pubkey,
		config: Option<RpcAccountInfoConfig>,
	) -> SubscribeResult<RpcResponse<UiAccount>> {
		let params = json!([pubkey.to_string(), config]);
		self.subscribe("account", params).await
	}

	pub async fn block_subscribe(
		&self,
		filter: RpcBlockSubscribeFilter,
		config: Option<RpcBlockSubscribeConfig>,
	) -> SubscribeResult<RpcResponse<RpcBlockUpdate>> {
		self.subscribe("block", json!([filter, config])).await
	}

	pub async fn logs_subscribe(
		&self,
		filter: RpcTransactionLogsFilter,
		config: RpcTransactionLogsConfig,
	) -> SubscribeResult<RpcResponse<RpcLogsResponse>> {
		self.subscribe("logs", json!([filter, config])).await
	}

	pub async fn program_subscribe(
		&self,
		pubkey: &Pubkey,
		config: Option<RpcProgramAccountsConfig>,
	) -> SubscribeResult<RpcResponse<RpcKeyedAccount>> {
		let params = json!([pubkey.to_string(), config]);
		self.subscribe("program", params).await
	}

	pub async fn vote_subscribe(&self) -> SubscribeResult<RpcVote> {
		self.subscribe("vote", json!([])).await
	}

	pub async fn root_subscribe(&self) -> SubscribeResult<Slot> {
		self.subscribe("root", json!([])).await
	}

	pub async fn signature_subscribe(
		&self,
		signature: &Signature,
		config: Option<RpcSignatureSubscribeConfig>,
	) -> SubscribeResult<RpcResponse<RpcSignatureResult>> {
		let params = json!([signature.to_string(), config]);
		self.subscribe("signature", params).await
	}

	pub async fn slot_subscribe(&self) -> SubscribeResult<SlotInfo> {
		self.subscribe("slot", json!([])).await
	}

	pub async fn slot_updates_subscribe(&self) -> SubscribeResult<SlotUpdate> {
		self.subscribe("slotsUpdates", json!([])).await
	}
}
