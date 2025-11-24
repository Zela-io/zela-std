//! This RPC Client is based on and should be interface compatible with <https://crates.io/crates/solana-rpc-client>.

use std::{
	str::FromStr,
	time::{Duration, Instant},
};

use base64::{Engine, prelude::BASE64_STANDARD};
use log::{debug, info, trace};
use serde_json::{Value, json};
use solana_account_decoder_client_types::{
	UiAccount, UiAccountData, UiAccountEncoding,
	token::{TokenAccountType, UiTokenAccount, UiTokenAmount},
};
use solana_sdk::{
	account::Account,
	bs58,
	clock::{DEFAULT_MS_PER_SLOT, Epoch, Slot, UnixTimestamp},
	commitment_config::CommitmentConfig,
	epoch_info::EpochInfo,
	epoch_schedule::EpochSchedule,
	hash::Hash,
	pubkey::Pubkey,
	signature::Signature,
	vote::state::MAX_LOCKOUT_HISTORY,
};
use solana_transaction_error::TransactionResult;
use solana_transaction_status_client_types::{
	EncodedConfirmedBlock, EncodedConfirmedTransactionWithStatusMeta, TransactionStatus,
	UiConfirmedBlock, UiTransactionEncoding,
};

use super::*;

async fn sleep(dur: Duration) {
	std::thread::sleep(dur);
}

#[derive(Default)]
pub struct RpcClientConfig {
	pub commitment_config: CommitmentConfig,
	pub confirm_transaction_initial_timeout: Option<Duration>,
}
impl RpcClientConfig {
	pub fn with_commitment(commitment_config: CommitmentConfig) -> Self {
		RpcClientConfig {
			commitment_config,
			..Self::default()
		}
	}
}

pub struct RpcClient {
	config: RpcClientConfig,
}
impl Default for RpcClient {
	fn default() -> Self {
		Self::new()
	}
}
impl RpcClient {
	pub fn new() -> Self {
		Self::new_with_commitment(CommitmentConfig::default())
	}

	pub fn new_with_commitment(commitment_config: CommitmentConfig) -> Self {
		Self {
			config: RpcClientConfig::with_commitment(commitment_config),
		}
	}

	pub fn new_with_config(config: RpcClientConfig) -> Self {
		Self { config }
	}

	#[deprecated(since = "2.0.2", note = "RpcClient::node_version is no longer used")]
	pub async fn set_node_version(&self, _version: semver::Version) -> Result<(), ()> {
		Ok(())
	}

	pub fn commitment(&self) -> CommitmentConfig {
		self.config.commitment_config
	}

	pub async fn send_and_confirm_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> ClientResult<Signature> {
		const SEND_RETRIES: usize = 1;
		const GET_STATUS_RETRIES: usize = usize::MAX;

		'sending: for _ in 0..SEND_RETRIES {
			let signature = self.send_transaction(transaction).await?;

			let recent_blockhash = if transaction.uses_durable_nonce() {
				let (recent_blockhash, ..) = self
					.get_latest_blockhash_with_commitment(CommitmentConfig::processed())
					.await?;
				recent_blockhash
			} else {
				*transaction.get_recent_blockhash()
			};

			for status_retry in 0..GET_STATUS_RETRIES {
				match self.get_signature_status(&signature).await? {
					Some(Ok(_)) => return Ok(signature),
					Some(Err(e)) => return Err(e.into()),
					None => {
						if !self
							.is_blockhash_valid(&recent_blockhash, CommitmentConfig::processed())
							.await?
						{
							// Block hash is not found by some reason
							break 'sending;
						} else if cfg!(not(test))
							// Ignore sleep at last step.
							&& status_retry < GET_STATUS_RETRIES
						{
							// Retry twice a second
							sleep(Duration::from_millis(500)).await;
							continue;
						}
					}
				}
			}
		}

		Err(RpcError::ForUser(
			"unable to confirm transaction. \
			 This can happen in situations such as transaction expiration \
			 and insufficient fee-payer funds"
				.to_string(),
		)
		.into())
	}

	pub async fn send_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> ClientResult<Signature> {
		self.send_transaction_with_config(
			transaction,
			RpcSendTransactionConfig {
				preflight_commitment: Some(self.commitment().commitment),
				..RpcSendTransactionConfig::default()
			},
		)
		.await
	}

	pub async fn send_transaction_with_config(
		&self,
		transaction: &impl SerializableTransaction,
		config: RpcSendTransactionConfig,
	) -> ClientResult<Signature> {
		let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);
		let preflight_commitment = CommitmentConfig {
			commitment: config.preflight_commitment.unwrap_or_default(),
		};
		let config = RpcSendTransactionConfig {
			encoding: Some(encoding),
			preflight_commitment: Some(preflight_commitment.commitment),
			..config
		};
		let serialized_encoded = serialize_and_encode(transaction, encoding)?;
		let signature_base58_str: String = match self
			.send(
				RpcRequest::SendTransaction,
				json!([serialized_encoded, config]),
			)
			.await
		{
			Ok(signature_base58_str) => signature_base58_str,
			Err(err) => {
				if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
					code,
					message,
					data,
				}) = &err.kind
				{
					debug!("{code} {message}");
					if let RpcResponseErrorData::SendTransactionPreflightFailure(r) = data {
						if let Some(ref logs) = r.logs {
							for (i, log) in logs.iter().enumerate() {
								debug!("{:>3}: {log}", i + 1);
							}
							debug!("");
						}
					}
				}
				return Err(err);
			}
		};

		let signature = signature_base58_str
			.parse::<Signature>()
			.map_err(|err| Into::<ClientError>::into(RpcError::ParseError(err.to_string())))?;
		// A mismatching RPC response signature indicates an issue with the RPC node, and
		// should not be passed along to confirmation methods. The transaction may or may
		// not have been submitted to the cluster, so callers should verify the success of
		// the correct transaction signature independently.
		if signature != *transaction.get_signature() {
			Err(RpcError::RpcRequestError(format!(
				"RPC node returned mismatched signature {:?}, expected {:?}",
				signature,
				transaction.get_signature()
			))
			.into())
		} else {
			Ok(*transaction.get_signature())
		}
	}

	pub async fn confirm_transaction(&self, signature: &Signature) -> ClientResult<bool> {
		Ok(self
			.confirm_transaction_with_commitment(signature, self.commitment())
			.await?
			.value)
	}

	pub async fn confirm_transaction_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> RpcResult<bool> {
		let Response { context, value } = self.get_signature_statuses(&[*signature]).await?;

		Ok(Response {
			context,
			value: value[0]
				.as_ref()
				.filter(|result| result.satisfies_commitment(commitment_config))
				.map(|result| result.status.is_ok())
				.unwrap_or_default(),
		})
	}

	pub async fn simulate_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> RpcResult<RpcSimulateTransactionResult> {
		self.simulate_transaction_with_config(
			transaction,
			RpcSimulateTransactionConfig {
				commitment: Some(self.commitment()),
				..RpcSimulateTransactionConfig::default()
			},
		)
		.await
	}

	pub async fn simulate_transaction_with_config(
		&self,
		transaction: &impl SerializableTransaction,
		config: RpcSimulateTransactionConfig,
	) -> RpcResult<RpcSimulateTransactionResult> {
		let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcSimulateTransactionConfig {
			encoding: Some(encoding),
			commitment: Some(commitment),
			..config
		};
		let serialized_encoded = serialize_and_encode(transaction, encoding)?;
		self.send(
			RpcRequest::SimulateTransaction,
			json!([serialized_encoded, config]),
		)
		.await
	}

	pub async fn get_highest_snapshot_slot(&self) -> ClientResult<RpcSnapshotSlotInfo> {
		self.send(RpcRequest::GetHighestSnapshotSlot, Value::Null)
			.await
	}

	pub async fn get_signature_status(
		&self,
		signature: &Signature,
	) -> ClientResult<Option<TransactionResult<()>>> {
		self.get_signature_status_with_commitment(signature, self.commitment())
			.await
	}

	pub async fn get_signature_statuses(
		&self,
		signatures: &[Signature],
	) -> RpcResult<Vec<Option<TransactionStatus>>> {
		let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
		self.send(RpcRequest::GetSignatureStatuses, json!([signatures]))
			.await
	}

	pub async fn get_signature_statuses_with_history(
		&self,
		signatures: &[Signature],
	) -> RpcResult<Vec<Option<TransactionStatus>>> {
		let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
		self.send(
			RpcRequest::GetSignatureStatuses,
			json!([signatures, {
				"searchTransactionHistory": true
			}]),
		)
		.await
	}

	pub async fn get_signature_status_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Option<TransactionResult<()>>> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()]]),
			)
			.await?;
		Ok(result.value[0]
			.clone()
			.filter(|result| result.satisfies_commitment(commitment_config))
			.map(|status_meta| status_meta.status))
	}

	pub async fn get_signature_status_with_commitment_and_history(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
		search_transaction_history: bool,
	) -> ClientResult<Option<TransactionResult<()>>> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()], {
					"searchTransactionHistory": search_transaction_history
				}]),
			)
			.await?;
		Ok(result.value[0]
			.clone()
			.filter(|result| result.satisfies_commitment(commitment_config))
			.map(|status_meta| status_meta.status))
	}

	pub async fn get_slot(&self) -> ClientResult<Slot> {
		self.get_slot_with_commitment(self.commitment()).await
	}

	pub async fn get_slot_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Slot> {
		self.send(RpcRequest::GetSlot, json!([commitment_config]))
			.await
	}

	pub async fn get_block_height(&self) -> ClientResult<u64> {
		self.get_block_height_with_commitment(self.commitment())
			.await
	}

	pub async fn get_block_height_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		self.send(RpcRequest::GetBlockHeight, json!([commitment_config]))
			.await
	}

	pub async fn get_slot_leaders(
		&self,
		start_slot: Slot,
		limit: u64,
	) -> ClientResult<Vec<Pubkey>> {
		self.send(RpcRequest::GetSlotLeaders, json!([start_slot, limit]))
			.await
			.and_then(|slot_leaders: Vec<String>| {
				slot_leaders
					.iter()
					.map(|slot_leader| {
						Pubkey::from_str(slot_leader).map_err(|err| {
							ClientErrorKind::Custom(format!("pubkey deserialization failed: {err}"))
								.into()
						})
					})
					.collect()
			})
	}

	pub async fn get_block_production(&self) -> RpcResult<RpcBlockProduction> {
		self.send(RpcRequest::GetBlockProduction, Value::Null).await
	}

	pub async fn get_block_production_with_config(
		&self,
		config: RpcBlockProductionConfig,
	) -> RpcResult<RpcBlockProduction> {
		self.send(RpcRequest::GetBlockProduction, json!([config]))
			.await
	}

	pub async fn supply(&self) -> RpcResult<RpcSupply> {
		self.supply_with_commitment(self.commitment()).await
	}

	pub async fn supply_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> RpcResult<RpcSupply> {
		self.send(RpcRequest::GetSupply, json!([commitment_config]))
			.await
	}

	pub async fn get_largest_accounts_with_config(
		&self,
		config: RpcLargestAccountsConfig,
	) -> RpcResult<Vec<RpcAccountBalance>> {
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcLargestAccountsConfig {
			commitment: Some(commitment),
			..config
		};
		self.send(RpcRequest::GetLargestAccounts, json!([config]))
			.await
	}

	pub async fn get_vote_accounts(&self) -> ClientResult<RpcVoteAccountStatus> {
		self.get_vote_accounts_with_commitment(self.commitment())
			.await
	}

	pub async fn get_vote_accounts_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<RpcVoteAccountStatus> {
		self.get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
			commitment: Some(commitment_config),
			..RpcGetVoteAccountsConfig::default()
		})
		.await
	}

	pub async fn get_vote_accounts_with_config(
		&self,
		config: RpcGetVoteAccountsConfig,
	) -> ClientResult<RpcVoteAccountStatus> {
		self.send(RpcRequest::GetVoteAccounts, json!([config]))
			.await
	}

	pub async fn wait_for_max_stake(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
	) -> ClientResult<()> {
		self.wait_for_max_stake_below_threshold_with_timeout_helper(
			commitment,
			max_stake_percent,
			None,
		)
		.await
	}

	pub async fn wait_for_max_stake_below_threshold_with_timeout(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
		timeout: Duration,
	) -> ClientResult<()> {
		self.wait_for_max_stake_below_threshold_with_timeout_helper(
			commitment,
			max_stake_percent,
			Some(timeout),
		)
		.await
	}

	async fn wait_for_max_stake_below_threshold_with_timeout_helper(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
		timeout: Option<Duration>,
	) -> ClientResult<()> {
		let mut current_percent;
		let start = Instant::now();
		loop {
			let vote_accounts = self.get_vote_accounts_with_commitment(commitment).await?;

			let mut max = 0;
			let total_active_stake = vote_accounts
				.current
				.iter()
				.chain(vote_accounts.delinquent.iter())
				.map(|vote_account| {
					max = std::cmp::max(max, vote_account.activated_stake);
					vote_account.activated_stake
				})
				.sum::<u64>();
			current_percent = 100f32 * max as f32 / total_active_stake as f32;
			if current_percent < max_stake_percent {
				break;
			} else if let Some(timeout) = timeout {
				if start.elapsed() > timeout {
					return Err(ClientErrorKind::Custom(
						"timed out waiting for max stake to drop".to_string(),
					)
					.into());
				}
			}

			info!(
				"Waiting for stake to drop below {max_stake_percent} current: {current_percent:.1}"
			);
			sleep(Duration::from_secs(5)).await;
		}
		Ok(())
	}

	pub async fn get_cluster_nodes(&self) -> ClientResult<Vec<RpcContactInfo>> {
		self.send(RpcRequest::GetClusterNodes, Value::Null).await
	}

	pub async fn get_block(&self, slot: Slot) -> ClientResult<EncodedConfirmedBlock> {
		self.get_block_with_encoding(slot, UiTransactionEncoding::Json)
			.await
	}

	pub async fn get_block_with_encoding(
		&self,
		slot: Slot,
		encoding: UiTransactionEncoding,
	) -> ClientResult<EncodedConfirmedBlock> {
		self.send(RpcRequest::GetBlock, json!([slot, encoding]))
			.await
	}

	pub async fn get_block_with_config(
		&self,
		slot: Slot,
		config: RpcBlockConfig,
	) -> ClientResult<UiConfirmedBlock> {
		self.send(RpcRequest::GetBlock, json!([slot, config])).await
	}

	pub async fn get_blocks(
		&self,
		start_slot: Slot,
		end_slot: Option<Slot>,
	) -> ClientResult<Vec<Slot>> {
		self.send(RpcRequest::GetBlocks, json!([start_slot, end_slot]))
			.await
	}

	pub async fn get_blocks_with_commitment(
		&self,
		start_slot: Slot,
		end_slot: Option<Slot>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Vec<Slot>> {
		let json = if end_slot.is_some() {
			json!([start_slot, end_slot, commitment_config])
		} else {
			json!([start_slot, commitment_config])
		};
		self.send(RpcRequest::GetBlocks, json).await
	}

	pub async fn get_blocks_with_limit(
		&self,
		start_slot: Slot,
		limit: usize,
	) -> ClientResult<Vec<Slot>> {
		self.send(RpcRequest::GetBlocksWithLimit, json!([start_slot, limit]))
			.await
	}

	pub async fn get_blocks_with_limit_and_commitment(
		&self,
		start_slot: Slot,
		limit: usize,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Vec<Slot>> {
		self.send(
			RpcRequest::GetBlocksWithLimit,
			json!([start_slot, limit, commitment_config]),
		)
		.await
	}

	pub async fn get_signatures_for_address(
		&self,
		address: &Pubkey,
	) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
		self.get_signatures_for_address_with_config(
			address,
			GetConfirmedSignaturesForAddress2Config::default(),
		)
		.await
	}

	pub async fn get_signatures_for_address_with_config(
		&self,
		address: &Pubkey,
		config: GetConfirmedSignaturesForAddress2Config,
	) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
		let config = RpcSignaturesForAddressConfig {
			before: config.before.map(|signature| signature.to_string()),
			until: config.until.map(|signature| signature.to_string()),
			limit: config.limit,
			commitment: config.commitment,
			min_context_slot: None,
		};

		let result: Vec<RpcConfirmedTransactionStatusWithSignature> = self
			.send(
				RpcRequest::GetSignaturesForAddress,
				json!([address.to_string(), config]),
			)
			.await?;

		Ok(result)
	}

	pub async fn get_transaction(
		&self,
		signature: &Signature,
		encoding: UiTransactionEncoding,
	) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
		self.send(
			RpcRequest::GetTransaction,
			json!([signature.to_string(), encoding]),
		)
		.await
	}

	pub async fn get_transaction_with_config(
		&self,
		signature: &Signature,
		config: RpcTransactionConfig,
	) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
		self.send(
			RpcRequest::GetTransaction,
			json!([signature.to_string(), config]),
		)
		.await
	}

	pub async fn get_block_time(&self, slot: Slot) -> ClientResult<UnixTimestamp> {
		let request = RpcRequest::GetBlockTime;
		let response = self.send(request, json!([slot])).await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(RpcError::ForUser(format!("Block Not Found: slot={slot}")).into());
				}
				let result = serde_json::from_value(result_json)
					.map_err(|err| ClientError::new_with_request(err.into(), request))?;
				trace!("Response block timestamp {slot:?} {result:?}");
				Ok(result)
			})
			.map_err(|err| err.into_with_request(request))?
	}

	pub async fn get_epoch_info(&self) -> ClientResult<EpochInfo> {
		self.get_epoch_info_with_commitment(self.commitment()).await
	}

	pub async fn get_epoch_info_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<EpochInfo> {
		self.send(RpcRequest::GetEpochInfo, json!([commitment_config]))
			.await
	}

	pub async fn get_leader_schedule(
		&self,
		slot: Option<Slot>,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.get_leader_schedule_with_commitment(slot, self.commitment())
			.await
	}

	pub async fn get_leader_schedule_with_commitment(
		&self,
		slot: Option<Slot>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.get_leader_schedule_with_config(
			slot,
			RpcLeaderScheduleConfig {
				commitment: Some(commitment_config),
				..RpcLeaderScheduleConfig::default()
			},
		)
		.await
	}

	pub async fn get_leader_schedule_with_config(
		&self,
		slot: Option<Slot>,
		config: RpcLeaderScheduleConfig,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.send(RpcRequest::GetLeaderSchedule, json!([slot, config]))
			.await
	}

	pub async fn get_epoch_schedule(&self) -> ClientResult<EpochSchedule> {
		self.send(RpcRequest::GetEpochSchedule, Value::Null).await
	}

	pub async fn get_recent_performance_samples(
		&self,
		limit: Option<usize>,
	) -> ClientResult<Vec<RpcPerfSample>> {
		self.send(RpcRequest::GetRecentPerformanceSamples, json!([limit]))
			.await
	}

	pub async fn get_recent_prioritization_fees(
		&self,
		addresses: &[Pubkey],
	) -> ClientResult<Vec<RpcPrioritizationFee>> {
		let addresses: Vec<_> = addresses
			.iter()
			.map(|address| address.to_string())
			.collect();
		self.send(RpcRequest::GetRecentPrioritizationFees, json!([addresses]))
			.await
	}

	pub async fn get_identity(&self) -> ClientResult<Pubkey> {
		let rpc_identity: RpcIdentity = self.send(RpcRequest::GetIdentity, Value::Null).await?;

		rpc_identity.identity.parse::<Pubkey>().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Pubkey".to_string()).into(),
				RpcRequest::GetIdentity,
			)
		})
	}

	pub async fn get_inflation_governor(&self) -> ClientResult<RpcInflationGovernor> {
		self.send(RpcRequest::GetInflationGovernor, Value::Null)
			.await
	}

	pub async fn get_inflation_rate(&self) -> ClientResult<RpcInflationRate> {
		self.send(RpcRequest::GetInflationRate, Value::Null).await
	}

	pub async fn get_inflation_reward(
		&self,
		addresses: &[Pubkey],
		epoch: Option<Epoch>,
	) -> ClientResult<Vec<Option<RpcInflationReward>>> {
		let addresses: Vec<_> = addresses
			.iter()
			.map(|address| address.to_string())
			.collect();
		self.send(
			RpcRequest::GetInflationReward,
			json!([
				addresses,
				RpcEpochConfig {
					epoch,
					commitment: Some(self.commitment()),
					min_context_slot: None,
				}
			]),
		)
		.await
	}

	pub async fn get_version(&self) -> ClientResult<RpcVersionInfo> {
		self.send(RpcRequest::GetVersion, Value::Null).await
	}

	pub async fn minimum_ledger_slot(&self) -> ClientResult<Slot> {
		self.send(RpcRequest::MinimumLedgerSlot, Value::Null).await
	}

	pub async fn get_account(&self, pubkey: &Pubkey) -> ClientResult<Account> {
		self.get_account_with_commitment(pubkey, self.commitment())
			.await?
			.value
			.ok_or_else(|| RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into())
	}

	pub async fn get_account_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Option<Account>> {
		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::Base64Zstd),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.get_account_with_config(pubkey, config).await
	}

	pub async fn get_account_with_config(
		&self,
		pubkey: &Pubkey,
		config: RpcAccountInfoConfig,
	) -> RpcResult<Option<Account>> {
		let response = self
			.send(
				RpcRequest::GetAccountInfo,
				json!([pubkey.to_string(), config]),
			)
			.await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(
						RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into(),
					);
				}
				let Response {
					context,
					value: rpc_account,
				} = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
				trace!("Response account {pubkey:?} {rpc_account:?}");
				let account = rpc_account.and_then(|rpc_account| rpc_account.decode());

				Ok(Response {
					context,
					value: account,
				})
			})
			.map_err(|err| {
				Into::<ClientError>::into(RpcError::ForUser(format!(
					"AccountNotFound: pubkey={pubkey}: {err}"
				)))
			})?
	}

	pub async fn get_max_retransmit_slot(&self) -> ClientResult<Slot> {
		self.send(RpcRequest::GetMaxRetransmitSlot, Value::Null)
			.await
	}

	pub async fn get_max_shred_insert_slot(&self) -> ClientResult<Slot> {
		self.send(RpcRequest::GetMaxShredInsertSlot, Value::Null)
			.await
	}

	pub async fn get_multiple_accounts(
		&self,
		pubkeys: &[Pubkey],
	) -> ClientResult<Vec<Option<Account>>> {
		Ok(self
			.get_multiple_accounts_with_commitment(pubkeys, self.commitment())
			.await?
			.value)
	}

	pub async fn get_multiple_accounts_with_commitment(
		&self,
		pubkeys: &[Pubkey],
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<Option<Account>>> {
		self.get_multiple_accounts_with_config(
			pubkeys,
			RpcAccountInfoConfig {
				encoding: Some(UiAccountEncoding::Base64Zstd),
				commitment: Some(commitment_config),
				data_slice: None,
				min_context_slot: None,
			},
		)
		.await
	}

	pub async fn get_multiple_accounts_with_config(
		&self,
		pubkeys: &[Pubkey],
		config: RpcAccountInfoConfig,
	) -> RpcResult<Vec<Option<Account>>> {
		let config = RpcAccountInfoConfig {
			commitment: config.commitment.or_else(|| Some(self.commitment())),
			..config
		};
		let pubkeys: Vec<_> = pubkeys.iter().map(|pubkey| pubkey.to_string()).collect();
		let response = self
			.send(RpcRequest::GetMultipleAccounts, json!([pubkeys, config]))
			.await?;
		let Response {
			context,
			value: accounts,
		} = serde_json::from_value::<Response<Vec<Option<UiAccount>>>>(response)?;
		let accounts: Vec<Option<Account>> = accounts
			.into_iter()
			.map(|rpc_account| rpc_account.and_then(|a| a.decode()))
			.collect();
		Ok(Response {
			context,
			value: accounts,
		})
	}

	pub async fn get_account_data(&self, pubkey: &Pubkey) -> ClientResult<Vec<u8>> {
		Ok(self.get_account(pubkey).await?.data)
	}

	pub async fn get_minimum_balance_for_rent_exemption(
		&self,
		data_len: usize,
	) -> ClientResult<u64> {
		let request = RpcRequest::GetMinimumBalanceForRentExemption;
		let minimum_balance_json: Value = self
			.send(request, json!([data_len]))
			.await
			.map_err(|err| err.into_with_request(request))?;

		let minimum_balance: u64 = serde_json::from_value(minimum_balance_json)
			.map_err(|err| ClientError::new_with_request(err.into(), request))?;
		trace!("Response minimum balance {data_len:?} {minimum_balance:?}");
		Ok(minimum_balance)
	}

	pub async fn get_balance(&self, pubkey: &Pubkey) -> ClientResult<u64> {
		Ok(self
			.get_balance_with_commitment(pubkey, self.commitment())
			.await?
			.value)
	}

	pub async fn get_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<u64> {
		self.send(
			RpcRequest::GetBalance,
			json!([pubkey.to_string(), commitment_config]),
		)
		.await
	}

	pub async fn get_program_accounts(
		&self,
		pubkey: &Pubkey,
	) -> ClientResult<Vec<(Pubkey, Account)>> {
		self.get_program_accounts_with_config(
			pubkey,
			RpcProgramAccountsConfig {
				account_config: RpcAccountInfoConfig {
					encoding: Some(UiAccountEncoding::Base64Zstd),
					..RpcAccountInfoConfig::default()
				},
				..RpcProgramAccountsConfig::default()
			},
		)
		.await
	}

	pub async fn get_program_accounts_with_config(
		&self,
		pubkey: &Pubkey,
		mut config: RpcProgramAccountsConfig,
	) -> ClientResult<Vec<(Pubkey, Account)>> {
		let commitment = config
			.account_config
			.commitment
			.unwrap_or_else(|| self.commitment());
		config.account_config.commitment = Some(commitment);

		let accounts = self
			.send::<OptionalContext<Vec<RpcKeyedAccount>>>(
				RpcRequest::GetProgramAccounts,
				json!([pubkey.to_string(), config]),
			)
			.await?
			.parse_value();
		parse_keyed_accounts(accounts, RpcRequest::GetProgramAccounts)
	}

	pub async fn get_stake_minimum_delegation(&self) -> ClientResult<u64> {
		self.get_stake_minimum_delegation_with_commitment(self.commitment())
			.await
	}

	pub async fn get_stake_minimum_delegation_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		Ok(self
			.send::<Response<u64>>(
				RpcRequest::GetStakeMinimumDelegation,
				json!([commitment_config]),
			)
			.await?
			.value)
	}

	pub async fn get_transaction_count(&self) -> ClientResult<u64> {
		self.get_transaction_count_with_commitment(self.commitment())
			.await
	}

	pub async fn get_transaction_count_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		self.send(RpcRequest::GetTransactionCount, json!([commitment_config]))
			.await
	}

	pub async fn get_first_available_block(&self) -> ClientResult<Slot> {
		self.send(RpcRequest::GetFirstAvailableBlock, Value::Null)
			.await
	}

	pub async fn get_genesis_hash(&self) -> ClientResult<Hash> {
		let hash_str: String = self.send(RpcRequest::GetGenesisHash, Value::Null).await?;
		let hash = hash_str.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Hash".to_string()).into(),
				RpcRequest::GetGenesisHash,
			)
		})?;
		Ok(hash)
	}

	pub async fn get_health(&self) -> ClientResult<()> {
		self.send::<String>(RpcRequest::GetHealth, Value::Null)
			.await
			.map(|_| ())
	}

	pub async fn get_token_account(&self, pubkey: &Pubkey) -> ClientResult<Option<UiTokenAccount>> {
		Ok(self
			.get_token_account_with_commitment(pubkey, self.commitment())
			.await?
			.value)
	}

	pub async fn get_token_account_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Option<UiTokenAccount>> {
		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};
		let response = self
			.send(
				RpcRequest::GetAccountInfo,
				json!([pubkey.to_string(), config]),
			)
			.await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(
						RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into(),
					);
				}
				let Response {
					context,
					value: rpc_account,
				} = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
				trace!("Response account {pubkey:?} {rpc_account:?}");
				let response = {
					if let Some(rpc_account) = rpc_account {
						if let UiAccountData::Json(account_data) = rpc_account.data {
							let token_account_type: TokenAccountType =
								serde_json::from_value(account_data.parsed)?;
							if let TokenAccountType::Account(token_account) = token_account_type {
								return Ok(Response {
									context,
									value: Some(token_account),
								});
							}
						}
					}
					Err(Into::<ClientError>::into(RpcError::ForUser(format!(
						"Account could not be parsed as token account: pubkey={pubkey}"
					))))
				};
				response?
			})
			.map_err(|err| {
				Into::<ClientError>::into(RpcError::ForUser(format!(
					"AccountNotFound: pubkey={pubkey}: {err}"
				)))
			})?
	}

	pub async fn get_token_account_balance(&self, pubkey: &Pubkey) -> ClientResult<UiTokenAmount> {
		Ok(self
			.get_token_account_balance_with_commitment(pubkey, self.commitment())
			.await?
			.value)
	}

	pub async fn get_token_account_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<UiTokenAmount> {
		self.send(
			RpcRequest::GetTokenAccountBalance,
			json!([pubkey.to_string(), commitment_config]),
		)
		.await
	}

	pub async fn get_token_accounts_by_delegate(
		&self,
		delegate: &Pubkey,
		token_account_filter: TokenAccountsFilter,
	) -> ClientResult<Vec<RpcKeyedAccount>> {
		Ok(self
			.get_token_accounts_by_delegate_with_commitment(
				delegate,
				token_account_filter,
				self.commitment(),
			)
			.await?
			.value)
	}

	pub async fn get_token_accounts_by_delegate_with_commitment(
		&self,
		delegate: &Pubkey,
		token_account_filter: TokenAccountsFilter,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<RpcKeyedAccount>> {
		let token_account_filter = match token_account_filter {
			TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
			TokenAccountsFilter::ProgramId(program_id) => {
				RpcTokenAccountsFilter::ProgramId(program_id.to_string())
			}
		};

		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.send(
			RpcRequest::GetTokenAccountsByOwner,
			json!([delegate.to_string(), token_account_filter, config]),
		)
		.await
	}

	pub async fn get_token_accounts_by_owner(
		&self,
		owner: &Pubkey,
		token_account_filter: TokenAccountsFilter,
	) -> ClientResult<Vec<RpcKeyedAccount>> {
		Ok(self
			.get_token_accounts_by_owner_with_commitment(
				owner,
				token_account_filter,
				self.commitment(),
			)
			.await?
			.value)
	}

	pub async fn get_token_accounts_by_owner_with_commitment(
		&self,
		owner: &Pubkey,
		token_account_filter: TokenAccountsFilter,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<RpcKeyedAccount>> {
		let token_account_filter = match token_account_filter {
			TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
			TokenAccountsFilter::ProgramId(program_id) => {
				RpcTokenAccountsFilter::ProgramId(program_id.to_string())
			}
		};

		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.send(
			RpcRequest::GetTokenAccountsByOwner,
			json!([owner.to_string(), token_account_filter, config]),
		)
		.await
	}

	pub async fn get_token_largest_accounts(
		&self,
		mint: &Pubkey,
	) -> ClientResult<Vec<RpcTokenAccountBalance>> {
		Ok(self
			.get_token_largest_accounts_with_commitment(mint, self.commitment())
			.await?
			.value)
	}

	pub async fn get_token_largest_accounts_with_commitment(
		&self,
		mint: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<RpcTokenAccountBalance>> {
		self.send(
			RpcRequest::GetTokenLargestAccounts,
			json!([mint.to_string(), commitment_config]),
		)
		.await
	}

	pub async fn get_token_supply(&self, mint: &Pubkey) -> ClientResult<UiTokenAmount> {
		Ok(self
			.get_token_supply_with_commitment(mint, self.commitment())
			.await?
			.value)
	}

	pub async fn get_token_supply_with_commitment(
		&self,
		mint: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<UiTokenAmount> {
		self.send(
			RpcRequest::GetTokenSupply,
			json!([mint.to_string(), commitment_config]),
		)
		.await
	}

	pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> ClientResult<Signature> {
		self.request_airdrop_with_config(
			pubkey,
			lamports,
			RpcRequestAirdropConfig {
				commitment: Some(self.commitment()),
				..RpcRequestAirdropConfig::default()
			},
		)
		.await
	}

	pub async fn request_airdrop_with_blockhash(
		&self,
		pubkey: &Pubkey,
		lamports: u64,
		recent_blockhash: &Hash,
	) -> ClientResult<Signature> {
		self.request_airdrop_with_config(
			pubkey,
			lamports,
			RpcRequestAirdropConfig {
				commitment: Some(self.commitment()),
				recent_blockhash: Some(recent_blockhash.to_string()),
			},
		)
		.await
	}

	pub async fn request_airdrop_with_config(
		&self,
		pubkey: &Pubkey,
		lamports: u64,
		config: RpcRequestAirdropConfig,
	) -> ClientResult<Signature> {
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcRequestAirdropConfig {
			commitment: Some(commitment),
			..config
		};
		self.send(
			RpcRequest::RequestAirdrop,
			json!([pubkey.to_string(), lamports, config]),
		)
		.await
		.and_then(|signature: String| {
			Signature::from_str(&signature).map_err(|err| {
				ClientErrorKind::Custom(format!("signature deserialization failed: {err}")).into()
			})
		})
		.map_err(|_| {
			RpcError::ForUser(
				"airdrop request failed. \
					This can happen when the rate limit is reached."
					.to_string(),
			)
			.into()
		})
	}

	pub(crate) async fn poll_balance_with_timeout_and_commitment(
		&self,
		pubkey: &Pubkey,
		polling_frequency: &Duration,
		timeout: &Duration,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		let now = Instant::now();
		loop {
			match self
				.get_balance_with_commitment(pubkey, commitment_config)
				.await
			{
				Ok(bal) => {
					return Ok(bal.value);
				}
				Err(e) => {
					sleep(*polling_frequency).await;
					if now.elapsed() > *timeout {
						return Err(e);
					}
				}
			};
		}
	}

	pub async fn poll_get_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		self.poll_balance_with_timeout_and_commitment(
			pubkey,
			&Duration::from_millis(100),
			&Duration::from_secs(1),
			commitment_config,
		)
		.await
	}

	pub async fn wait_for_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		expected_balance: Option<u64>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		const LAST: usize = 30;
		let mut run = 0;
		loop {
			let balance_result = self
				.poll_get_balance_with_commitment(pubkey, commitment_config)
				.await;
			if expected_balance.is_none() || (balance_result.is_err() && run == LAST) {
				return balance_result;
			}
			trace!(
				"wait_for_balance_with_commitment [{run}] {balance_result:?} {expected_balance:?}"
			);
			if let (Some(expected_balance), Ok(balance_result)) = (expected_balance, balance_result)
			{
				if expected_balance == balance_result {
					return Ok(balance_result);
				}
			}
			run += 1;
		}
	}

	pub async fn poll_for_signature(&self, signature: &Signature) -> ClientResult<()> {
		self.poll_for_signature_with_commitment(signature, self.commitment())
			.await
	}

	pub async fn poll_for_signature_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> ClientResult<()> {
		let now = Instant::now();
		loop {
			if let Ok(Some(_)) = self
				.get_signature_status_with_commitment(signature, commitment_config)
				.await
			{
				break;
			}
			if now.elapsed().as_secs() > 15 {
				return Err(RpcError::ForUser(format!(
					"signature not found after {} seconds",
					now.elapsed().as_secs()
				))
				.into());
			}
			sleep(Duration::from_millis(250)).await;
		}
		Ok(())
	}

	pub async fn poll_for_signature_confirmation(
		&self,
		signature: &Signature,
		min_confirmed_blocks: usize,
	) -> ClientResult<usize> {
		let mut now = Instant::now();
		let mut confirmed_blocks = 0;
		loop {
			let response = self
				.get_num_blocks_since_signature_confirmation(signature)
				.await;
			match response {
				Ok(count) => {
					if confirmed_blocks != count {
						info!(
							"signature {} confirmed {} out of {} after {} ms",
							signature,
							count,
							min_confirmed_blocks,
							now.elapsed().as_millis()
						);
						now = Instant::now();
						confirmed_blocks = count;
					}
					if count >= min_confirmed_blocks {
						break;
					}
				}
				Err(err) => {
					debug!("check_confirmations request failed: {err:?}");
				}
			};
			if now.elapsed().as_secs() > 20 {
				info!(
					"signature {} confirmed {} out of {} failed after {} ms",
					signature,
					confirmed_blocks,
					min_confirmed_blocks,
					now.elapsed().as_millis()
				);
				if confirmed_blocks > 0 {
					return Ok(confirmed_blocks);
				} else {
					return Err(RpcError::ForUser(format!(
						"signature not found after {} seconds",
						now.elapsed().as_secs()
					))
					.into());
				}
			}
			sleep(Duration::from_millis(250)).await;
		}
		Ok(confirmed_blocks)
	}

	pub async fn get_num_blocks_since_signature_confirmation(
		&self,
		signature: &Signature,
	) -> ClientResult<usize> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()]]),
			)
			.await?;

		let confirmations = result.value[0]
			.clone()
			.ok_or_else(|| {
				ClientError::new_with_request(
					ClientErrorKind::Custom("signature not found".to_string()),
					RpcRequest::GetSignatureStatuses,
				)
			})?
			.confirmations
			.unwrap_or(MAX_LOCKOUT_HISTORY + 1);
		Ok(confirmations)
	}

	pub async fn get_latest_blockhash(&self) -> ClientResult<Hash> {
		let (blockhash, _) = self
			.get_latest_blockhash_with_commitment(self.commitment())
			.await?;
		Ok(blockhash)
	}

	pub async fn get_latest_blockhash_with_commitment(
		&self,
		commitment: CommitmentConfig,
	) -> ClientResult<(Hash, u64)> {
		let RpcBlockhash {
			blockhash,
			last_valid_block_height,
		} = self
			.send::<Response<RpcBlockhash>>(RpcRequest::GetLatestBlockhash, json!([commitment]))
			.await?
			.value;
		let blockhash = blockhash.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Hash".to_string()).into(),
				RpcRequest::GetLatestBlockhash,
			)
		})?;
		Ok((blockhash, last_valid_block_height))
	}

	pub async fn is_blockhash_valid(
		&self,
		blockhash: &Hash,
		commitment: CommitmentConfig,
	) -> ClientResult<bool> {
		Ok(self
			.send::<Response<bool>>(
				RpcRequest::IsBlockhashValid,
				json!([blockhash.to_string(), commitment,]),
			)
			.await?
			.value)
	}

	pub async fn get_fee_for_message(
		&self,
		message: &impl SerializableMessage,
	) -> ClientResult<u64> {
		let serialized_encoded = serialize_and_encode(message, UiTransactionEncoding::Base64)?;
		let result = self
			.send::<Response<Option<u64>>>(
				RpcRequest::GetFeeForMessage,
				json!([serialized_encoded, self.commitment()]),
			)
			.await?;
		result
			.value
			.ok_or_else(|| ClientErrorKind::Custom("Invalid blockhash".to_string()).into())
	}

	pub async fn get_new_latest_blockhash(&self, blockhash: &Hash) -> ClientResult<Hash> {
		let mut num_retries = 0;
		let start = Instant::now();
		while start.elapsed().as_secs() < 5 {
			if let Ok(new_blockhash) = self.get_latest_blockhash().await {
				if new_blockhash != *blockhash {
					return Ok(new_blockhash);
				}
			}
			debug!("Got same blockhash ({blockhash:?}), will retry...");

			// Retry ~twice during a slot
			sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2)).await;
			num_retries += 1;
		}
		Err(RpcError::ForUser(format!(
			"Unable to get new blockhash after {}ms (retried {} times), stuck at {}",
			start.elapsed().as_millis(),
			num_retries,
			blockhash
		))
		.into())
	}

	pub async fn send<T: serde::de::DeserializeOwned>(
		&self,
		request: RpcRequest,
		params: Value,
	) -> ClientResult<T> {
		assert!(params.is_object() || params.is_array() || params.is_null());

		let method = format!("{request}");
		let response = match crate::call_rpc::<Value, Value, Value>(&method, params) {
			Ok(Ok(v)) => v,
			Ok(Err(err)) => {
				return Err(ClientError {
					request: Some(request),
					kind: ClientErrorKind::Custom(format!(
						"RPC call return error {}: {}",
						err.code, err.message
					)),
				});
			}
			Err(err) => {
				return Err(ClientError {
					request: Some(request),
					kind: ClientErrorKind::Custom(format!("RPC call failed: {err}")),
				});
			}
		};

		serde_json::from_value(response)
			.map_err(|err| ClientError::new_with_request(err.into(), request))
	}
}

fn serialize_and_encode<T>(input: &T, encoding: UiTransactionEncoding) -> ClientResult<String>
where
	T: serde::ser::Serialize,
{
	let serialized = bincode::serialize(input)
		.map_err(|e| ClientErrorKind::Custom(format!("Serialization failed: {e}")))?;
	let encoded = match encoding {
		UiTransactionEncoding::Base58 => bs58::encode(serialized).into_string(),
		UiTransactionEncoding::Base64 => BASE64_STANDARD.encode(serialized),
		_ => {
			return Err(ClientErrorKind::Custom(format!(
				"unsupported encoding: {encoding}. Supported encodings: base58, base64"
			))
			.into());
		}
	};
	Ok(encoded)
}

pub(crate) fn parse_keyed_accounts(
	accounts: Vec<RpcKeyedAccount>,
	request: RpcRequest,
) -> ClientResult<Vec<(Pubkey, Account)>> {
	let mut pubkey_accounts: Vec<(Pubkey, Account)> = Vec::with_capacity(accounts.len());
	for RpcKeyedAccount { pubkey, account } in accounts.into_iter() {
		let pubkey = pubkey.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Pubkey".to_string()).into(),
				request,
			)
		})?;
		pubkey_accounts.push((
			pubkey,
			account.decode().ok_or_else(|| {
				ClientError::new_with_request(
					RpcError::ParseError("Account from rpc".to_string()).into(),
					request,
				)
			})?,
		));
	}
	Ok(pubkey_accounts)
}
