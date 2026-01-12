use std::collections::HashMap;

use solana_sdk::bs58;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CompiledInstruction, Message, SubscribeUpdate,
    SubscribeUpdateAccount, SubscribeUpdateTransaction, TransactionStatusMeta,
};

const PUMPSWAP_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

pub struct PumpSwapProcessor {
    known_pools: HashMap<String, (String, String)>,
}

impl PumpSwapProcessor {
    pub fn new() -> Self {
        Self {
            known_pools: HashMap::new(),
        }
    }

    pub fn handle_update(&mut self, update: SubscribeUpdate) {
        match update.update_oneof {
            Some(UpdateOneof::Account(account_update)) => {
                self.handle_account_update(account_update);
            }
            Some(UpdateOneof::Transaction(tx_update)) => {
                self.handle_transaction_update(tx_update);
            }
            _ => {}
        }
    }

    fn handle_account_update(&mut self, account_update: SubscribeUpdateAccount) {
        if let Some(account) = &account_update.account {
            let owner = bs58::encode(&account.owner).into_string();
            let account_address = bs58::encode(&account.pubkey).into_string();

            if owner == PUMPSWAP_PROGRAM_ID {
                if account.data.len() >= 107 {
                    let discriminator = &account.data[0..8];
                    if discriminator == &POOL_DISCRIMINATOR {
                        let base_mint = bs58::encode(&account.data[43..75]).into_string();
                        let quote_mint = bs58::encode(&account.data[75..107]).into_string();
                        self.known_pools
                            .insert(account_address, (base_mint, quote_mint));
                    }
                }
            }
        }
    }

    fn handle_transaction_update(&mut self, tx_update: SubscribeUpdateTransaction) {
        if let Some(tx) = &tx_update.transaction {
            if let Some(meta) = &tx.meta {
                if let Some(message) = &tx.transaction {
                    if let Some(tx_message) = &message.message {
                        for instruction in &tx_message.instructions {
                            let program_id_index = instruction.program_id_index as usize;
                            if program_id_index < tx_message.account_keys.len() {
                                let program_id_bytes = &tx_message.account_keys[program_id_index];
                                let program_id = bs58::encode(program_id_bytes).into_string();

                                if program_id == PUMPSWAP_PROGRAM_ID {
                                    self.parse_pumpswap_instruction(instruction, tx_message, meta);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn parse_pumpswap_instruction(
        &mut self,
        instruction: &CompiledInstruction,
        message: &Message,
        meta: &TransactionStatusMeta,
    ) {
        let data = &instruction.data;

        if data.len() < 8 {
            return;
        }

        let discriminator = &data[0..8];

        if discriminator == &BUY_DISCRIMINATOR {
            println!("PUMPSWAP BUY INSTRUCTION DETECTED");
            self.extract_pumpswap_price_from_reserves(instruction, message, meta, "buy");
        } else if discriminator == &SELL_DISCRIMINATOR {
            println!("PUMPSWAP SELL INSTRUCTION DETECTED");
            self.extract_pumpswap_price_from_reserves(instruction, message, meta, "sell");
        }
    }

    fn extract_pumpswap_price_from_reserves(
        &mut self,
        instruction: &CompiledInstruction,
        message: &Message,
        meta: &TransactionStatusMeta,
        instruction_type: &str,
    ) {
        if instruction.accounts.is_empty() {
            return;
        }

        let pool_account_index = instruction.accounts[0] as usize;
        if pool_account_index >= message.account_keys.len() {
            return;
        }

        let pool_address_bytes = &message.account_keys[pool_account_index];
        let pool_address = bs58::encode(pool_address_bytes).into_string();

        println!(
            "Processing PumpSwap {} for pool: {}",
            instruction_type, pool_address
        );

        let (base_mint, _quote_mint) =
            if let Some((base, quote)) = self.known_pools.get(&pool_address) {
                (base.clone(), quote.clone())
            } else {
                ("".to_string(), "".to_string())
            };

        let mut token_reserve: f64 = 0.0;
        let mut sol_reserve: f64 = 0.0;
        let mut detected_token_mint = String::new();

        for balance in &meta.pre_token_balances {
            if balance.owner == pool_address {
                let mint = &balance.mint;
                let amount = balance
                    .ui_token_amount
                    .as_ref()
                    .map(|a| a.ui_amount)
                    .unwrap_or(0.0);

                if mint == "So11111111111111111111111111111111111111112" {
                    sol_reserve = amount;
                } else {
                    token_reserve = amount;
                    detected_token_mint = mint.clone();
                }

                println!(
                    "  - {} reserve: {} (mint: {})",
                    if mint == "So11111111111111111111111111111111111111112" {
                        "SOL"
                    } else {
                        "Token"
                    },
                    amount,
                    mint
                );
            }
        }

        let final_token_mint = if !base_mint.is_empty()
            && base_mint != "So11111111111111111111111111111111111111112"
        {
            base_mint
        } else {
            detected_token_mint
        };

        if token_reserve > 0.0 && sol_reserve > 0.0 {
            let price_per_token = sol_reserve / token_reserve;
            let tokens_per_sol = token_reserve / sol_reserve;

            println!("CURRENT PRICE: {:.9} SOL per token", price_per_token);
            println!("CURRENT PRICE: {:.2} tokens per SOL", tokens_per_sol);
            println!("RESERVES: {} tokens, {} SOL", token_reserve, sol_reserve);

            if final_token_mint != "So11111111111111111111111111111111111111112"
                && !final_token_mint.is_empty()
            {
                println!(
                    "Price update: {} = {:.9} SOL per token",
                    final_token_mint, price_per_token
                );
                println!("   Pool: {}", pool_address);
                println!("   Timestamp: {:?}", std::time::SystemTime::now());
            } else {
                println!("Invalid token mint: {}", final_token_mint);
            }
        } else {
            println!("Insufficient reserve data for price calculation");
            println!(
                "   Token Reserve: {}, SOL Reserve: {}",
                token_reserve, sol_reserve
            );
        }

        println!("------------------------");
    }
}
