use crate::models::Account;
use crate::modules::{account, config, logger, quota};
use chrono::Utc;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use tauri::Manager;
use tokio::time::{self, Duration};

// Warmup history: key = "email:model_name:100", value = warmup timestamp
static WARMUP_HISTORY: Lazy<Mutex<HashMap<String, i64>>> =
    Lazy::new(|| Mutex::new(load_warmup_history()));

fn get_warmup_history_path() -> Result<PathBuf, String> {
    let data_dir = account::get_data_dir()?;
    Ok(data_dir.join("warmup_history.json"))
}

fn load_warmup_history() -> HashMap<String, i64> {
    match get_warmup_history_path() {
        Ok(path) if path.exists() => match std::fs::read_to_string(&path) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => HashMap::new(),
        },
        _ => HashMap::new(),
    }
}

fn save_warmup_history(history: &HashMap<String, i64>) {
    if let Ok(path) = get_warmup_history_path() {
        if let Ok(content) = serde_json::to_string_pretty(history) {
            let _ = std::fs::write(&path, content);
        }
    }
}

pub fn record_warmup_history(key: &str, timestamp: i64) {
    let mut history = WARMUP_HISTORY.lock().unwrap();
    history.insert(key.to_string(), timestamp);
    save_warmup_history(&history);
}

pub fn check_cooldown(key: &str, cooldown_seconds: i64) -> bool {
    let history = WARMUP_HISTORY.lock().unwrap();
    if let Some(&last_ts) = history.get(key) {
        let now = chrono::Utc::now().timestamp();
        now - last_ts < cooldown_seconds
    } else {
        false
    }
}

pub fn start_scheduler(
    app_handle: Option<tauri::AppHandle>,
    proxy_state: crate::commands::proxy::ProxyServiceState,
) {
    tauri::async_runtime::spawn(async move {
        logger::log_info("[Scheduler] Background Task Scheduler started.");

        // Check tasks every 60 seconds (1 minute)
        let mut interval = time::interval(Duration::from_secs(60));

        let mut last_refresh_run: Option<chrono::DateTime<Utc>> = None;
        let mut last_sync_run: Option<chrono::DateTime<Utc>> = None;
        let mut last_warmup_run: Option<chrono::DateTime<Utc>> = None;

        loop {
            interval.tick().await;

            // Load configuration
            let app_config = match config::load_app_config() {
                Ok(cfg) => cfg,
                Err(_) => continue,
            };

            let now = Utc::now();

            // 1. Background Auto Refresh of Quotas
            if app_config.auto_refresh && app_config.refresh_interval > 0 {
                let should_refresh = match last_refresh_run {
                    None => {
                        // Initialize timer to now (avoid immediate run on start/enable,
                        // as UI does it or we want to wait for the interval)
                        last_refresh_run = Some(now);
                        false
                    }
                    Some(last) => {
                        let elapsed = now.signed_duration_since(last).num_minutes();
                        elapsed >= app_config.refresh_interval as i64
                    }
                };

                if should_refresh {
                    logger::log_info(&format!(
                        "[Scheduler] Triggering background auto-refresh (every {}m)...",
                        app_config.refresh_interval
                    ));
                    let state_clone = proxy_state.clone();
                    let handle_clone = app_handle.clone();
                    tokio::spawn(async move {
                        let _ = crate::commands::refresh_all_quotas_internal(
                            &state_clone,
                            handle_clone,
                        )
                        .await;
                    });
                    last_refresh_run = Some(now);
                }
            } else {
                last_refresh_run = None;
            }

            // 2. Background Auto Sync of Current Account from DB
            if app_config.auto_sync && app_config.sync_interval > 0 {
                let should_sync = match last_sync_run {
                    None => {
                        last_sync_run = Some(now);
                        false
                    }
                    Some(last) => {
                        let elapsed = now.signed_duration_since(last).num_minutes();
                        elapsed >= app_config.sync_interval as i64
                    }
                };

                if should_sync {
                    logger::log_info(&format!(
                        "[Scheduler] Triggering background auto-sync (every {}m)...",
                        app_config.sync_interval
                    ));
                    let state_clone = proxy_state.clone();
                    let handle_clone = app_handle.clone();
                    tokio::spawn(async move {
                        if let Some(handle) = handle_clone {
                            let state_ref =
                                handle.state::<crate::commands::proxy::ProxyServiceState>();
                            match crate::commands::sync_account_from_db(handle.clone(), state_ref)
                                .await
                            {
                                Ok(Some(account)) => {
                                    logger::log_info(&format!(
                                        "[Scheduler] Auto-sync success, switched to: {}",
                                        account.email
                                    ));
                                    use tauri::Emitter;
                                    let _ = handle.emit("accounts://refreshed", ());
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    logger::log_error(&format!(
                                        "[Scheduler] Auto-sync failed: {}",
                                        e
                                    ));
                                }
                            }
                        }
                    });
                    last_sync_run = Some(now);
                }
            } else {
                last_sync_run = None;
            }

            // 3. Smart Warmup (if enabled)
            if app_config.scheduled_warmup.enabled {
                let should_warmup = match last_warmup_run {
                    None => {
                        last_warmup_run = Some(now);
                        false
                    }
                    Some(last) => {
                        let elapsed = now.signed_duration_since(last).num_minutes();
                        elapsed >= 10 // Scan every 10 minutes
                    }
                };

                if should_warmup {
                    // Get all accounts
                    let accounts = match account::list_accounts() {
                        Ok(accs) => accs,
                        Err(_) => {
                            last_warmup_run = Some(now);
                            continue;
                        }
                    };

                    if !accounts.is_empty() {
                        logger::log_info(&format!(
                            "[Scheduler] Scanning {} accounts for 100% quota models...",
                            accounts.len()
                        ));

                        let mut warmup_tasks = Vec::new();
                        let mut skipped_cooldown = 0;

                        for account in &accounts {
                            let Ok((token, pid)) = quota::get_valid_token_for_warmup(account).await
                            else {
                                continue;
                            };

                            let Ok((fresh_quota, _)) = quota::fetch_quota_with_cache(
                                &token,
                                &account.email,
                                Some(&pid),
                                Some(&account.id),
                            )
                            .await
                            else {
                                continue;
                            };

                            if fresh_quota.is_forbidden {
                                logger::log_warn(&format!(
                                    "[Scheduler] Account {} returned 403 Forbidden during quota fetch, marking as forbidden",
                                    account.email
                                ));
                                let _ = account::mark_account_forbidden(
                                    &account.id,
                                    "Scheduler: 403 Forbidden - quota fetch denied",
                                );
                                continue;
                            }

                            let now_ts = Utc::now().timestamp();

                            for model in fresh_quota.models {
                                if model.percentage == 100 {
                                    let model_to_ping = model.name.clone();

                                    if !app_config
                                        .scheduled_warmup
                                        .monitored_models
                                        .contains(&model_to_ping)
                                    {
                                        continue;
                                    }

                                    let history_key =
                                        format!("{}:{}:100", account.email, model_to_ping);

                                    {
                                        let history = WARMUP_HISTORY.lock().unwrap();
                                        if let Some(&last_warmup_ts) = history.get(&history_key) {
                                            let cooldown_seconds = 14400;
                                            if now_ts - last_warmup_ts < cooldown_seconds {
                                                skipped_cooldown += 1;
                                                continue;
                                            }
                                        }
                                    }

                                    warmup_tasks.push((
                                        account.id.clone(),
                                        account.email.clone(),
                                        model_to_ping.clone(),
                                        token.clone(),
                                        pid.clone(),
                                        model.percentage,
                                        history_key.clone(),
                                    ));

                                    logger::log_info(&format!(
                                        "[Scheduler] ✓ Scheduled warmup: {} @ {} (quota at 100%)",
                                        model_to_ping, account.email
                                    ));
                                } else if model.percentage < 100 {
                                    let model_to_ping = model.name.clone();
                                    let history_key =
                                        format!("{}:{}:100", account.email, model_to_ping);

                                    let mut history = WARMUP_HISTORY.lock().unwrap();
                                    if history.remove(&history_key).is_some() {
                                        save_warmup_history(&history);
                                        logger::log_info(&format!(
                                            "[Scheduler] Cleared history for {} @ {} (quota: {}%)",
                                            model_to_ping, account.email, model.percentage
                                        ));
                                    }
                                }
                            }
                        }

                        if !warmup_tasks.is_empty() {
                            let total = warmup_tasks.len();
                            if skipped_cooldown > 0 {
                                logger::log_info(&format!(
                                    "[Scheduler] Skipped {} models in cooldown, will warmup {}",
                                    skipped_cooldown, total
                                ));
                            }
                            logger::log_info(&format!(
                                "[Scheduler] 🔥 Triggering {} warmup tasks...",
                                total
                            ));

                            let handle_for_warmup = app_handle.clone();
                            let state_for_warmup = proxy_state.clone();

                            tokio::spawn(async move {
                                let mut success = 0;
                                let batch_size = 3;
                                let now_ts = chrono::Utc::now().timestamp();

                                for (batch_idx, batch) in
                                    warmup_tasks.chunks(batch_size).enumerate()
                                {
                                    let mut handles = Vec::new();

                                    for (
                                        task_idx,
                                        (id, email, model, token, pid, pct, history_key),
                                    ) in batch.iter().enumerate()
                                    {
                                        let global_idx = batch_idx * batch_size + task_idx + 1;
                                        let id = id.clone();
                                        let email = email.clone();
                                        let model = model.clone();
                                        let token = token.clone();
                                        let pid = pid.clone();
                                        let pct = *pct;
                                        let history_key = history_key.clone();

                                        logger::log_info(&format!(
                                            "[Warmup {}/{}] {} @ {} ({}%)",
                                            global_idx, total, model, email, pct
                                        ));

                                        let handle = tokio::spawn(async move {
                                            let result = quota::warmup_model_directly(
                                                &token,
                                                &model,
                                                &pid,
                                                &email,
                                                pct,
                                                Some(&id),
                                            )
                                            .await;
                                            (result, history_key)
                                        });
                                        handles.push(handle);
                                    }

                                    for handle in handles {
                                        match handle.await {
                                            Ok((true, history_key)) => {
                                                success += 1;
                                                record_warmup_history(&history_key, now_ts);
                                            }
                                            _ => {}
                                        }
                                    }

                                    if batch_idx
                                        < (warmup_tasks.len() + batch_size - 1) / batch_size - 1
                                    {
                                        tokio::time::sleep(tokio::time::Duration::from_secs(2))
                                            .await;
                                    }
                                }

                                logger::log_info(&format!(
                                    "[Scheduler] ✅ Warmup completed: {}/{} successful",
                                    success, total
                                ));

                                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                                let _ = crate::commands::refresh_all_quotas_internal(
                                    &state_for_warmup,
                                    handle_for_warmup,
                                )
                                .await;
                            });
                        } else if skipped_cooldown > 0 {
                            logger::log_info(&format!(
                                "[Scheduler] Scan completed, all 100% models are in cooldown, skipped {}",
                                skipped_cooldown
                            ));
                        } else {
                            logger::log_info(
                                "[Scheduler] Scan completed, no models with 100% quota need warmup",
                            );
                        }
                    }
                    last_warmup_run = Some(now);
                }
            } else {
                last_warmup_run = None;
            }

            // Regularly clean up history (keep last 24 hours)
            {
                let now_ts = Utc::now().timestamp();
                let mut history = WARMUP_HISTORY.lock().unwrap();
                let cutoff = now_ts - 86400; // 24 hours ago
                history.retain(|_, &mut ts| ts > cutoff);
            }
        }
    });
}

/// Trigger immediate smart warmup check for a single account
pub async fn trigger_warmup_for_account(account: &Account) {
    // Get valid token
    let Ok((token, pid)) = quota::get_valid_token_for_warmup(account).await else {
        return;
    };

    // Get quota info (prefer cache as refresh command likely just updated disk/cache)
    let Ok((fresh_quota, _)) =
        quota::fetch_quota_with_cache(&token, &account.email, Some(&pid), Some(&account.id)).await
    else {
        return;
    };

    // [FIX] 预热阶段检测到 403 时，使用统一禁用逻辑，确保账号文件和索引同时更新
    if fresh_quota.is_forbidden {
        logger::log_warn(&format!(
            "[Scheduler] Account {} returned 403 Forbidden during quota fetch, marking as forbidden",
            account.email
        ));
        let _ = account::mark_account_forbidden(
            &account.id,
            "Scheduler: 403 Forbidden - quota fetch denied",
        );
        return;
    }

    // Load config once at the beginning
    let Ok(app_config) = config::load_app_config() else {
        logger::log_warn("[Scheduler] Failed to load app config, skipping warmup check");
        return;
    };

    let now_ts = Utc::now().timestamp();
    let mut tasks_to_run = Vec::new();

    for model in fresh_quota.models {
        let model_name = model.name.clone();
        let history_key = format!("{}:{}:100", account.email, model_name);

        if model.percentage == 100 {
            // First check if model is in user's monitored list
            if !app_config
                .scheduled_warmup
                .monitored_models
                .contains(&model_name)
            {
                continue;
            }

            // Then check cooldown history
            {
                let history = WARMUP_HISTORY.lock().unwrap();

                // 4 hour cooldown (Pro account resets every 5h, 1h margin)
                if let Some(&last_warmup_ts) = history.get(&history_key) {
                    let cooldown_seconds = 14400;
                    if now_ts - last_warmup_ts < cooldown_seconds {
                        // Still in cooldown, skip
                        continue;
                    }
                }
            }
            // Note: Don't write history here - only write after successful warmup

            tasks_to_run.push((model_name, model.percentage, history_key));
        } else if model.percentage < 100 {
            // Quota not full, clear history, allow warmup next time it's 100%
            let mut history = WARMUP_HISTORY.lock().unwrap();
            if history.remove(&history_key).is_some() {
                save_warmup_history(&history);
            }
        }
    }

    // Execute warmup and record history only on success
    if !tasks_to_run.is_empty() {
        logger::log_info(&format!(
            "[Scheduler] Found {} models ready for warmup on {}",
            tasks_to_run.len(),
            account.email
        ));

        for (model, pct, history_key) in tasks_to_run {
            logger::log_info(&format!(
                "[Scheduler] 🔥 Triggering individual warmup: {} @ {} (Sync)",
                model, account.email
            ));

            let success = quota::warmup_model_directly(
                &token,
                &model,
                &pid,
                &account.email,
                pct,
                Some(&account.id),
            )
            .await;

            // Only record history if warmup was successful
            if success {
                record_warmup_history(&history_key, now_ts);
            }
        }
    }
}
