use serde::Serialize;

use solana_message::{Message as LegacyMessage, v0};
use solana_sdk::{commitment_config::CommitmentConfig, hash::Hash, signature::Signature};
use solana_transaction::{Transaction, uses_durable_nonce, versioned::VersionedTransaction};

/// Trait used to add support for versioned messages to RPC APIs while
/// retaining backwards compatibility
pub trait SerializableMessage: Serialize {}
impl SerializableMessage for LegacyMessage {}
impl SerializableMessage for v0::Message {}

/// Trait used to add support for versioned transactions to RPC APIs while
/// retaining backwards compatibility
pub trait SerializableTransaction: Serialize {
	fn get_signature(&self) -> &Signature;
	fn get_recent_blockhash(&self) -> &Hash;
	fn uses_durable_nonce(&self) -> bool;
}
impl SerializableTransaction for Transaction {
	fn get_signature(&self) -> &Signature {
		&self.signatures[0]
	}
	fn get_recent_blockhash(&self) -> &Hash {
		&self.message.recent_blockhash
	}
	fn uses_durable_nonce(&self) -> bool {
		uses_durable_nonce(self).is_some()
	}
}
impl SerializableTransaction for VersionedTransaction {
	fn get_signature(&self) -> &Signature {
		&self.signatures[0]
	}
	fn get_recent_blockhash(&self) -> &Hash {
		self.message.recent_blockhash()
	}
	fn uses_durable_nonce(&self) -> bool {
		self.uses_durable_nonce()
	}
}

#[derive(Debug, Default)]
pub struct GetConfirmedSignaturesForAddress2Config {
	pub before: Option<Signature>,
	pub until: Option<Signature>,
	pub limit: Option<usize>,
	pub commitment: Option<CommitmentConfig>,
}
