use std::collections::HashMap;

use solana_sdk::bs58;
use spl_token::instruction::TokenInstruction as TokenInstructionLegacy;
use spl_token_2022::instruction::TokenInstruction as TokenInstruction2022;
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, InnerInstruction, Message, SubscribeUpdate, SubscribeUpdateTransaction,
    TransactionStatusMeta, subscribe_update::UpdateOneof,
};

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Clone, Debug)]
struct BalanceInfo {
    mint: String,
    decimals: u8,
    pre_ui: Option<f64>,
    post_ui: Option<f64>,
}

pub struct TokenProcessor;

impl TokenProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn handle_update(&mut self, update: SubscribeUpdate) {
        if let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof {
            self.handle_transaction_update(tx_update);
        }
    }

    fn handle_transaction_update(&mut self, tx_update: SubscribeUpdateTransaction) {
        let Some(tx) = &tx_update.transaction else {
            return;
        };
        let Some(meta) = &tx.meta else {
            return;
        };
        let Some(message) = &tx.transaction else {
            return;
        };
        let Some(tx_message) = &message.message else {
            return;
        };

        let signature = bs58::encode(&tx.signature).into_string();
        let balances = build_balance_map(meta);

        for (idx, instruction) in tx_message.instructions.iter().enumerate() {
            self.process_compiled_instruction(
                tx_message,
                meta,
                instruction,
                &balances,
                tx_update.slot,
                &signature,
                idx as u32,
            );
        }

        for inner in &meta.inner_instructions {
            for (inner_idx, instruction) in inner.instructions.iter().enumerate() {
                self.process_inner_instruction(
                    tx_message,
                    meta,
                    instruction,
                    &balances,
                    tx_update.slot,
                    &signature,
                    inner.index,
                    inner_idx as u32,
                );
            }
        }
    }

    fn process_compiled_instruction(
        &self,
        message: &Message,
        meta: &TransactionStatusMeta,
        instruction: &CompiledInstruction,
        balances: &HashMap<u32, BalanceInfo>,
        slot: u64,
        signature: &str,
        instruction_index: u32,
    ) {
        self.process_token_instruction(
            message,
            meta,
            instruction.program_id_index as usize,
            &instruction.accounts,
            &instruction.data,
            balances,
            slot,
            signature,
            false,
            instruction_index,
            None,
        );
    }

    fn process_inner_instruction(
        &self,
        message: &Message,
        meta: &TransactionStatusMeta,
        instruction: &InnerInstruction,
        balances: &HashMap<u32, BalanceInfo>,
        slot: u64,
        signature: &str,
        parent_index: u32,
        inner_index: u32,
    ) {
        self.process_token_instruction(
            message,
            meta,
            instruction.program_id_index as usize,
            &instruction.accounts,
            &instruction.data,
            balances,
            slot,
            signature,
            true,
            parent_index,
            Some(inner_index),
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn process_token_instruction(
        &self,
        message: &Message,
        _meta: &TransactionStatusMeta,
        program_id_index: usize,
        accounts: &[u8],
        data: &[u8],
        balances: &HashMap<u32, BalanceInfo>,
        slot: u64,
        signature: &str,
        is_inner: bool,
        instruction_index: u32,
        inner_index: Option<u32>,
    ) {
        let program_id = match account_key(message, program_id_index) {
            Some(id) => id,
            None => return,
        };

        if program_id != TOKEN_PROGRAM_ID && program_id != TOKEN_2022_PROGRAM_ID {
            return;
        }

        if program_id == TOKEN_PROGRAM_ID {
            let Ok(instruction) = TokenInstructionLegacy::unpack(data) else {
                return;
            };
            self.log_token_instruction_legacy(
                message,
                accounts,
                balances,
                slot,
                signature,
                &program_id,
                instruction,
                is_inner,
                instruction_index,
                inner_index,
            );
        } else {
            let Ok(instruction) = TokenInstruction2022::unpack(data) else {
                return;
            };
            self.log_token_instruction_2022(
                message,
                accounts,
                balances,
                slot,
                signature,
                &program_id,
                instruction,
                is_inner,
                instruction_index,
                inner_index,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn log_token_instruction_legacy(
        &self,
        message: &Message,
        accounts: &[u8],
        balances: &HashMap<u32, BalanceInfo>,
        slot: u64,
        signature: &str,
        program_id: &str,
        instruction: TokenInstructionLegacy,
        is_inner: bool,
        instruction_index: u32,
        inner_index: Option<u32>,
    ) {
        match instruction {
            TokenInstructionLegacy::Transfer { amount } => {
                let source = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 0)
                    .or_else(|| decimals_for_account(balances, accounts, 1));
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 1))
                    .or_else(|| balance_delta_ui(balances, accounts, 0).map(|v| v.abs()));
                let mint = mint_for_account(balances, accounts, 0)
                    .or_else(|| mint_for_account(balances, accounts, 1));

                log_event(
                    "transfer",
                    slot,
                    signature,
                    program_id,
                    source,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstructionLegacy::TransferChecked { amount, decimals } => {
                let source = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let destination = account_from_indices(message, accounts, 2);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "transfer_checked",
                    slot,
                    signature,
                    program_id,
                    source,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstructionLegacy::MintTo { amount } => {
                let mint = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 1);
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 1));

                log_event(
                    "mint_to",
                    slot,
                    signature,
                    program_id,
                    None,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstructionLegacy::MintToChecked { amount, decimals } => {
                let mint = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "mint_to_checked",
                    slot,
                    signature,
                    program_id,
                    None,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstructionLegacy::Burn { amount } => {
                let account = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 0);
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 0).map(|v| v.abs()));

                log_event(
                    "burn",
                    slot,
                    signature,
                    program_id,
                    account,
                    None,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstructionLegacy::BurnChecked { amount, decimals } => {
                let account = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "burn_checked",
                    slot,
                    signature,
                    program_id,
                    account,
                    None,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            _ => {}
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn log_token_instruction_2022(
        &self,
        message: &Message,
        accounts: &[u8],
        balances: &HashMap<u32, BalanceInfo>,
        slot: u64,
        signature: &str,
        program_id: &str,
        instruction: TokenInstruction2022,
        is_inner: bool,
        instruction_index: u32,
        inner_index: Option<u32>,
    ) {
        match instruction {
            TokenInstruction2022::Transfer { amount } => {
                let source = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 0)
                    .or_else(|| decimals_for_account(balances, accounts, 1));
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 1))
                    .or_else(|| balance_delta_ui(balances, accounts, 0).map(|v| v.abs()));
                let mint = mint_for_account(balances, accounts, 0)
                    .or_else(|| mint_for_account(balances, accounts, 1));

                log_event(
                    "transfer",
                    slot,
                    signature,
                    program_id,
                    source,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstruction2022::TransferChecked { amount, decimals } => {
                let source = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let destination = account_from_indices(message, accounts, 2);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "transfer_checked",
                    slot,
                    signature,
                    program_id,
                    source,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstruction2022::MintTo { amount } => {
                let mint = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 1);
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 1));

                log_event(
                    "mint_to",
                    slot,
                    signature,
                    program_id,
                    None,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstruction2022::MintToChecked { amount, decimals } => {
                let mint = account_from_indices(message, accounts, 0);
                let destination = account_from_indices(message, accounts, 1);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "mint_to_checked",
                    slot,
                    signature,
                    program_id,
                    None,
                    destination,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstruction2022::Burn { amount } => {
                let account = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let decimals = decimals_for_account(balances, accounts, 0);
                let ui_amount = ui_amount_from(amount, decimals)
                    .or_else(|| balance_delta_ui(balances, accounts, 0).map(|v| v.abs()));

                log_event(
                    "burn",
                    slot,
                    signature,
                    program_id,
                    account,
                    None,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            TokenInstruction2022::BurnChecked { amount, decimals } => {
                let account = account_from_indices(message, accounts, 0);
                let mint = account_from_indices(message, accounts, 1);
                let ui_amount = ui_amount_from(amount, Some(decimals));

                log_event(
                    "burn_checked",
                    slot,
                    signature,
                    program_id,
                    account,
                    None,
                    mint,
                    amount,
                    ui_amount,
                    is_inner,
                    instruction_index,
                    inner_index,
                );
            }
            _ => {}
        }
    }
}

fn build_balance_map(meta: &TransactionStatusMeta) -> HashMap<u32, BalanceInfo> {
    let mut balances: HashMap<u32, BalanceInfo> = HashMap::new();

    for balance in &meta.pre_token_balances {
        let decimals = balance
            .ui_token_amount
            .as_ref()
            .map(|amount| amount.decimals as u8)
            .unwrap_or(0);
        let entry = balances
            .entry(balance.account_index)
            .or_insert_with(|| BalanceInfo {
                mint: balance.mint.clone(),
                decimals,
                pre_ui: None,
                post_ui: None,
            });
        entry.mint = balance.mint.clone();
        entry.decimals = decimals;
        entry.pre_ui = balance
            .ui_token_amount
            .as_ref()
            .map(|amount| amount.ui_amount);
    }

    for balance in &meta.post_token_balances {
        let decimals = balance
            .ui_token_amount
            .as_ref()
            .map(|amount| amount.decimals as u8)
            .unwrap_or(0);
        let entry = balances
            .entry(balance.account_index)
            .or_insert_with(|| BalanceInfo {
                mint: balance.mint.clone(),
                decimals,
                pre_ui: None,
                post_ui: None,
            });
        entry.mint = balance.mint.clone();
        entry.decimals = decimals;
        entry.post_ui = balance
            .ui_token_amount
            .as_ref()
            .map(|amount| amount.ui_amount);
    }

    balances
}

fn account_key(message: &Message, index: usize) -> Option<String> {
    message
        .account_keys
        .get(index)
        .map(|key| bs58::encode(key).into_string())
}

fn account_from_indices(message: &Message, accounts: &[u8], position: usize) -> Option<String> {
    let index = *accounts.get(position)? as usize;
    account_key(message, index)
}

fn balance_delta_ui(
    balances: &HashMap<u32, BalanceInfo>,
    accounts: &[u8],
    position: usize,
) -> Option<f64> {
    let index = *accounts.get(position)? as u32;
    let info = balances.get(&index)?;
    match (info.pre_ui, info.post_ui) {
        (Some(pre), Some(post)) => Some(post - pre),
        _ => None,
    }
}

fn decimals_for_account(
    balances: &HashMap<u32, BalanceInfo>,
    accounts: &[u8],
    position: usize,
) -> Option<u8> {
    let index = *accounts.get(position)? as u32;
    balances.get(&index).map(|info| info.decimals)
}

fn mint_for_account(
    balances: &HashMap<u32, BalanceInfo>,
    accounts: &[u8],
    position: usize,
) -> Option<String> {
    let index = *accounts.get(position)? as u32;
    balances.get(&index).map(|info| info.mint.clone())
}

fn ui_amount_from(amount: u64, decimals: Option<u8>) -> Option<f64> {
    let decimals = decimals?;
    let divisor = 10_f64.powi(decimals as i32);
    Some(amount as f64 / divisor)
}

#[allow(clippy::too_many_arguments)]
fn log_event(
    kind: &str,
    slot: u64,
    signature: &str,
    program_id: &str,
    source: Option<String>,
    destination: Option<String>,
    mint: Option<String>,
    amount: u64,
    ui_amount: Option<f64>,
    is_inner: bool,
    instruction_index: u32,
    inner_index: Option<u32>,
) {
    let src = source.unwrap_or_else(|| "unknown".to_string());
    let dst = destination.unwrap_or_else(|| "unknown".to_string());
    let mint = mint.unwrap_or_else(|| "unknown".to_string());
    let inner = if is_inner { "inner" } else { "outer" };
    let inner_index = inner_index
        .map(|idx| idx.to_string())
        .unwrap_or_else(|| "-".to_string());
    let ui = ui_amount
        .map(|value| format!("{value:.9}"))
        .unwrap_or_else(|| "unknown".to_string());
    println!(
        "----------------------------------------\nTOKEN {kind}\n  slot: {slot}\n  sig: {signature}\n  program: {program_id}\n  source: {src}\n  dest: {dst}\n  mint: {mint}\n  amount: {amount}\n  ui_amount: {ui}\n  loc: {inner}:{instruction_index}:{inner_index}"
    );
}
