// 上游客户端实现
// 基于高性能通讯接口封装

use dashmap::DashMap;
use rquest::{header, Client, Response, StatusCode};
use serde_json::Value;
use std::sync::Arc;
use std::{fs::OpenOptions, io::Write, path::Path};
use tokio::sync::RwLock;
use tokio::time::Duration;

/// The local test harness waits longer than the upstream request deadline so it
/// can receive the proxy's timeout result. A client timeout still cannot prove
/// that Google's service cancelled work already accepted upstream.
pub const LIVE_IMAGE_UPSTREAM_TIMEOUT_SECS: u64 = 540;
pub const LIVE_IMAGE_CLIENT_TIMEOUT_SECS: u64 = 600;

#[derive(Clone, Debug)]
struct NativeImageLogContext {
    correlation_id: String,
    model: String,
    image_size: String,
}

fn safe_log_model(value: &str) -> String {
    let safe: String = value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || "-_.:".contains(*character))
        .take(100)
        .collect();
    if safe.is_empty() {
        "unknown".to_string()
    } else {
        safe
    }
}

fn native_image_log_context(body: &Value) -> NativeImageLogContext {
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .map(safe_log_model)
        .unwrap_or_else(|| "unknown".to_string());
    let image_size = body
        .pointer("/request/generationConfig/imageConfig/imageSize")
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "1K" | "2K" | "4K"))
        .unwrap_or("unknown")
        .to_string();
    NativeImageLogContext {
        correlation_id: uuid::Uuid::new_v4().to_string(),
        model,
        image_size,
    }
}

fn native_image_event_path(budget_db_path: &Path, correlation_id: &str) -> std::path::PathBuf {
    budget_db_path.with_extension(format!("events-{correlation_id}.jsonl"))
}

fn record_native_image_event_at(
    budget_db_path: &Path,
    context: &NativeImageLogContext,
    event: &str,
    attempt: Option<u64>,
    http_status: Option<u16>,
    elapsed_ms: Option<u128>,
    error_kind: Option<&str>,
) -> Result<(), String> {
    let timestamp_utc = chrono::Utc::now().to_rfc3339();
    let record = serde_json::json!({
        "timestamp_utc": timestamp_utc,
        "event": event,
        "correlation_id": context.correlation_id,
        "model": context.model,
        "imageSize": context.image_size,
        "attempt": attempt,
        "http_status": http_status,
        "elapsed_ms": elapsed_ms,
        "error_kind": error_kind,
    });
    let path = native_image_event_path(budget_db_path, &context.correlation_id);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|_| "native image event log unavailable".to_string())?;
    serde_json::to_writer(&mut file, &record)
        .map_err(|_| "native image event log serialization failed".to_string())?;
    file.write_all(b"\n")
        .and_then(|_| file.sync_all())
        .map_err(|_| "native image event log write failed".to_string())?;

    tracing::info!(
        timestamp_utc = %timestamp_utc,
        event = %event,
        correlation_id = %context.correlation_id,
        model = %context.model,
        imageSize = %context.image_size,
        attempt = ?attempt,
        http_status = ?http_status,
        elapsed_ms = ?elapsed_ms,
        error_kind = ?error_kind,
        "native image upstream state"
    );
    Ok(())
}

/// Mark that the image handler fully read and parsed the upstream JSON response.
/// The response body itself is deliberately never persisted by this logger.
pub fn record_native_image_response_saved(
    correlation_id: &str,
    model: &str,
    image_size: &str,
) -> Result<(), String> {
    let budget_db_path = crate::proxy::image_attempt_budget::current_profile_budget_path()
        .map_err(|_| "native image profile unavailable".to_string())?;
    record_native_image_response_saved_at(&budget_db_path, correlation_id, model, image_size)
}

fn record_native_image_response_saved_at(
    budget_db_path: &Path,
    correlation_id: &str,
    model: &str,
    image_size: &str,
) -> Result<(), String> {
    let context = NativeImageLogContext {
        correlation_id: correlation_id.to_string(),
        model: safe_log_model(model),
        image_size: if matches!(image_size, "1K" | "2K" | "4K") {
            image_size.to_string()
        } else {
            "unknown".to_string()
        },
    };
    record_native_image_event_at(
        budget_db_path,
        &context,
        "RESPONSE_SAVED",
        None,
        None,
        None,
        None,
    )
}

pub fn record_native_image_response_failure(
    correlation_id: &str,
    model: &str,
    image_size: &str,
    timed_out: bool,
    elapsed_ms: u128,
) -> Result<(), String> {
    let budget_db_path = crate::proxy::image_attempt_budget::current_profile_budget_path()
        .map_err(|_| "native image profile unavailable".to_string())?;
    let context = NativeImageLogContext {
        correlation_id: correlation_id.to_string(),
        model: safe_log_model(model),
        image_size: if matches!(image_size, "1K" | "2K" | "4K") {
            image_size.to_string()
        } else {
            "unknown".to_string()
        },
    };
    record_native_image_event_at(
        &budget_db_path,
        &context,
        if timed_out {
            "UPSTREAM_TIMEOUT"
        } else {
            "UPSTREAM_ERROR"
        },
        None,
        None,
        Some(elapsed_ms),
        Some(if timed_out {
            "timeout_reading_response"
        } else {
            "response_read"
        }),
    )
}

/// 端点降级尝试的记录信息
#[derive(Debug, Clone)]
pub struct FallbackAttemptLog {
    /// 尝试的端点 URL
    pub endpoint_url: String,
    /// HTTP 状态码 (网络错误时为 None)
    pub status: Option<u16>,
    /// 错误描述
    pub error: String,
}

/// 上游调用结果，包含响应和降级尝试记录
pub struct UpstreamCallResult {
    /// 最终的 HTTP 响应
    pub response: Response,
    /// 降级过程中失败的端点尝试记录 (成功时为空)
    pub fallback_attempts: Vec<FallbackAttemptLog>,
    /// Random, non-secret correlation id used only by native image test logs.
    pub image_correlation_id: Option<String>,
}

struct NativeImageSendGuard {
    budget_db_path: std::path::PathBuf,
    context: NativeImageLogContext,
    attempt: u64,
    finished: bool,
}

impl NativeImageSendGuard {
    fn finish(&mut self) {
        self.finished = true;
    }
}

impl Drop for NativeImageSendGuard {
    fn drop(&mut self) {
        if !self.finished {
            let _ = record_native_image_event_at(
                &self.budget_db_path,
                &self.context,
                "UPSTREAM_INTERRUPTED",
                Some(self.attempt),
                None,
                None,
                Some("local_future_cancelled"),
            );
        }
    }
}

fn live_image_request_timeout() -> Duration {
    #[cfg(test)]
    {
        Duration::from_millis(250)
    }
    #[cfg(not(test))]
    {
        Duration::from_secs(LIVE_IMAGE_UPSTREAM_TIMEOUT_SECS)
    }
}

/// 邮箱脱敏：只显示前3位 + *** + @域名前2位 + ***
/// 例: "userexample@gmail.com" → "use***@gm***"
pub fn mask_email(email: &str) -> String {
    if let Some(at_pos) = email.find('@') {
        let local = &email[..at_pos];
        let domain = &email[at_pos + 1..];
        let local_prefix: String = local.chars().take(3).collect();
        let domain_prefix: String = domain.chars().take(2).collect();
        format!("{}***@{}***", local_prefix, domain_prefix)
    } else {
        // 不是合法邮箱格式，直接截取前5位
        let prefix: String = email.chars().take(5).collect();
        format!("{}***", prefix)
    }
}

/// [NEW] 错误日志脱敏：抹除报错信息中的 access_token, proxy_url 等敏感凭证
pub fn sanitize_error_for_log(error_text: &str) -> String {
    // 抹除常见敏感 key 的值
    let re = regex::Regex::new(r#"(?i)(access_token|refresh_token|id_token|authorization|api_key|secret|password|proxy_url|http_proxy|https_proxy)\s*[:=]\s*[^"'\\\s,}\]]+"#).unwrap();
    let redacted = re.replace_all(error_text, "$1=<redacted>");

    // 抹除 Bearer token
    let re_bearer = regex::Regex::new(r#"(?i)(bearer\s+)[^"'\\\s,}\]]+"#).unwrap();
    let redacted = re_bearer.replace_all(&redacted, "$1<redacted>");

    // 限制长度防止日志炸弹 (UTF-8 字符边界安全保护)
    if redacted.len() > 1000 {
        format!(
            "{}... (truncated)",
            crate::proxy::mappers::common_utils::safe_truncate_str(&redacted, 1000)
        )
    } else {
        redacted.into_owned()
    }
}

// Cloud Code v1internal endpoints (fallback order: Daily → Sandbox → Prod)
//
// Daily 优先 —— 它是官方 IDE 原生唯一主力端点（`language_server` 的启动参数即指向它），
// 稳定支持思维链与工具调用；Sandbox 为沙箱备用、Prod 为生产兜底，
// 后两者均易触发 Prod 环境的 429（Ref: Issue #1176, Issue #3523）。
//
// [FIX Issue #3525 / PR #3526] 顺序同时是**正确性**要求，不只是可用性偏好：
// 同一账号 / 模型 / 代理下，Sandbox 对部分地区返回**终止性 400**
// `User location is not supported for the API use.`，而 `should_try_next_endpoint`
// 只对 408 / 404 / 5xx 回退 → 400 不触发回退，故 Sandbox 排首位会让已验证可用的
// Daily 永远不被尝试。Sandbox 保留为回退项、回退判定规则不变 ——
// **不对所有 400 无条件重试**。
const V1_INTERNAL_BASE_URL_PROD: &str = "https://cloudcode-pa.googleapis.com/v1internal";
const V1_INTERNAL_BASE_URL_DAILY: &str = "https://daily-cloudcode-pa.googleapis.com/v1internal";
const V1_INTERNAL_BASE_URL_SANDBOX: &str =
    "https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal";

const V1_INTERNAL_BASE_URL_FALLBACKS: [&str; 3] = [
    V1_INTERNAL_BASE_URL_DAILY, // 优先级 1: Daily (官方 IDE 原生唯一主力端点，稳定支持思维链与工具调用)
    V1_INTERNAL_BASE_URL_SANDBOX, // 优先级 2: Sandbox (沙箱备用；部分地区对合规账号返回终止性 400)
    V1_INTERNAL_BASE_URL_PROD,  // 优先级 3: Prod (生产兜底，易触发 429)
];

pub struct UpstreamClient {
    default_client: RwLock<Client>,
    proxy_pool: Option<Arc<crate::proxy::proxy_pool::ProxyPoolManager>>,
    client_cache: DashMap<String, Client>, // proxy_id -> Client
    user_agent_override: RwLock<Option<String>>,
}

fn configured_http2_retry_count() -> usize {
    if cfg!(feature = "native-image-live-test") {
        0
    } else {
        2 // rquest's normal default, retained for the production build.
    }
}

impl UpstreamClient {
    pub fn new(
        proxy_config: Option<crate::proxy::config::UpstreamProxyConfig>,
        proxy_pool: Option<Arc<crate::proxy::proxy_pool::ProxyPoolManager>>,
    ) -> Self {
        // The test binary owns the attempt boundary. Do not route through a
        // separately configured forwarding proxy that could retry POST requests
        // outside this process's durable counter.
        let (proxy_config, proxy_pool) = if cfg!(feature = "native-image-live-test") {
            (None, None)
        } else {
            (proxy_config, proxy_pool)
        };
        let default_client = match Self::build_client_internal(proxy_config.clone()) {
            Ok(client) => client,
            Err(err_with_proxy) => {
                tracing::error!(
                    error = %err_with_proxy,
                    "Failed to create default HTTP client with configured upstream proxy; retrying without proxy"
                );
                match Self::build_client_internal(None) {
                    Ok(client) => client,
                    Err(err_without_proxy) => {
                        if cfg!(feature = "native-image-live-test") {
                            panic!("Safe live-test HTTP client unavailable; refusing an unbounded client: {err_without_proxy}");
                        }
                        tracing::error!(
                            error = %err_without_proxy,
                            "Failed to create default HTTP client without proxy; falling back to bare client"
                        );
                        Client::new()
                    }
                }
            }
        };

        Self {
            default_client: RwLock::new(default_client),
            proxy_pool,
            client_cache: DashMap::new(),
            user_agent_override: RwLock::new(None),
        }
    }

    /// [HOT-RELOAD] Rebuild the default HTTP client using the supplied upstream
    /// proxy config. Called from `update_proxy` so changes to the upstream proxy
    /// take effect without restarting the app.
    pub async fn rebuild_default_client(
        &self,
        proxy_config: Option<crate::proxy::config::UpstreamProxyConfig>,
    ) {
        let new_client = match Self::build_client_internal(proxy_config.clone()) {
            Ok(c) => c,
            Err(err_with_proxy) => {
                tracing::error!(
                    error = %err_with_proxy,
                    "Hot-reload: failed to rebuild default HTTP client with configured upstream proxy; retrying without proxy"
                );
                match Self::build_client_internal(None) {
                    Ok(c) => c,
                    Err(err_without_proxy) => {
                        tracing::error!(
                            error = %err_without_proxy,
                            "Hot-reload: failed to rebuild default HTTP client without proxy; keeping previous client"
                        );
                        return;
                    }
                }
            }
        };
        let mut guard = self.default_client.write().await;
        *guard = new_client;
        tracing::info!("UpstreamClient default_client rebuilt (upstream proxy hot-reloaded)");
    }

    /// [HOT-RELOAD] Drop all per-proxy cached clients. Call after the pool
    /// configuration changes (proxy URL/credentials edited, proxy removed,
    /// bindings changed) so the next request rebuilds with fresh settings.
    pub fn clear_client_cache(&self) {
        let size = self.client_cache.len();
        self.client_cache.clear();
        if size > 0 {
            tracing::info!("UpstreamClient cleared {} cached per-proxy clients", size);
        }
    }

    /// Base client builder configured with common connection pool, timeouts, and TCP keep-alive
    fn base_client_builder() -> rquest::ClientBuilder {
        let builder = Client::builder()
            .emulation(rquest_util::Emulation::Chrome123)
            // Connection settings (优化连接复用，减少建立开销)
            .connect_timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(20) // 每主机最多 20 个空闲连接 (对齐官方指纹)
            .pool_idle_timeout(Duration::from_secs(90)) // 空闲连接保持 90 秒
            // [FIX #3593 / #3571] 彻底恢复 Chrome 123 原生协议行为：
            // 1. 实测证实 Google GFE 对客户端 HTTP/2 PING 帧静默丢弃、不回复 PING ACK，
            //    导致 hyper2 在 keep_alive_timeout 到期时本地自杀式掐死连接并抛出 error reading a body from connection；
            // 2. 移除破坏性的底层 HTTP/2 PING 配置，回归 4.8.8 稳态基线；
            // 3. 依赖标准 L4 TCP Keep-Alive (60s) 保活，下游已有 PR #3578 的 3s SSE 注释心跳防客户端超时。
            .tcp_keepalive(Duration::from_secs(60)) // TCP 稳态保活探测 (60秒)
            // 强制开启 HTTP/2 协议，并支持在 SOCKS/HTTPS 代理下通过 ALPN 强制降级/协商
            .timeout(Duration::from_secs(600));

        // rquest otherwise permits hidden HTTP/2 retries (default: 2). The
        // dedicated live-test binary disables them so one send() reservation
        // cannot fan out into additional generateContent HTTP requests.
        let builder = builder.http2_max_retry_count(configured_http2_retry_count());
        let builder = if cfg!(feature = "native-image-live-test") {
            // Redirect following can issue a second HTTP request invisibly inside
            // one send(); keep redirects outside the image attempt boundary.
            builder
                .no_proxy()
                .redirect(rquest::redirect::Policy::none())
        } else {
            builder
        };

        Self::apply_default_user_agent(builder)
    }

    /// Internal helper to build a client with optional upstream proxy config
    fn build_client_internal(
        proxy_config: Option<crate::proxy::config::UpstreamProxyConfig>,
    ) -> Result<Client, rquest::Error> {
        let proxy_config = if cfg!(feature = "native-image-live-test") {
            None
        } else {
            proxy_config
        };
        let mut builder = Self::base_client_builder();

        if let Some(config) = proxy_config {
            if config.enabled && !config.url.is_empty() {
                let url = crate::proxy::config::normalize_proxy_url(&config.url);
                if let Ok(proxy) = rquest::Proxy::all(&url) {
                    builder = builder.proxy(proxy);
                    tracing::info!("UpstreamClient enabled proxy: {}", url);
                }
            }
        }

        builder.build()
    }

    /// Build a client with a specific PoolProxyConfig (from ProxyPool)
    fn build_client_with_proxy(
        &self,
        proxy_config: crate::proxy::proxy_pool::PoolProxyConfig,
    ) -> Result<Client, rquest::Error> {
        // Reuse base settings similar to default client but with specific proxy
        let builder = Self::base_client_builder().proxy(proxy_config.proxy);
        builder.build()
    }

    fn apply_default_user_agent(builder: rquest::ClientBuilder) -> rquest::ClientBuilder {
        let ua = crate::constants::USER_AGENT.as_str();
        if header::HeaderValue::from_str(ua).is_ok() {
            builder.user_agent(ua)
        } else {
            tracing::warn!(
                user_agent = %ua,
                "Invalid default User-Agent value, using fallback"
            );
            builder.user_agent("antigravity")
        }
    }

    /// Set dynamic User-Agent override
    pub async fn set_user_agent_override(&self, ua: Option<String>) {
        let mut lock = self.user_agent_override.write().await;
        *lock = ua;
        tracing::debug!("UpstreamClient User-Agent override updated: {:?}", lock);
    }

    /// Get current User-Agent (sanitized with safety floor >= 4.3.0)
    pub async fn get_user_agent(&self) -> String {
        let ua_override = self.user_agent_override.read().await;
        match ua_override.as_ref() {
            Some(ua) => crate::constants::sanitize_egress_user_agent(ua),
            None => crate::constants::USER_AGENT.clone(),
        }
    }

    /// Get client for a specific account (or default if no proxy bound)
    pub async fn get_client(&self, account_id: Option<&str>) -> Client {
        if let Some(pool) = &self.proxy_pool {
            if let Some(acc_id) = account_id {
                // Try to get per-account proxy
                match pool.get_proxy_for_account(acc_id).await {
                    Ok(Some(proxy_cfg)) => {
                        // Check cache
                        if let Some(client) = self.client_cache.get(&proxy_cfg.entry_id) {
                            return client.clone();
                        }
                        // Build new client and cache it
                        match self.build_client_with_proxy(proxy_cfg.clone()) {
                            Ok(client) => {
                                self.client_cache
                                    .insert(proxy_cfg.entry_id.clone(), client.clone());
                                tracing::info!(
                                    "Using ProxyPool proxy ID: {} for account: {}",
                                    proxy_cfg.entry_id,
                                    acc_id
                                );
                                return client;
                            }
                            Err(e) => {
                                tracing::error!("Failed to build client for proxy {}: {}, falling back to default", proxy_cfg.entry_id, e);
                            }
                        }
                    }
                    Ok(None) => {
                        // No proxy found or required for this account, use default
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error getting proxy for account {}: {}, falling back to default",
                            acc_id,
                            e
                        );
                    }
                }
            }
        }
        // Fallback to default client
        self.default_client.read().await.clone()
    }

    /// Build v1internal URL
    fn build_url(base_url: &str, method: &str, query_string: Option<&str>) -> String {
        if let Some(qs) = query_string {
            format!("{}:{}?{}", base_url, method, qs)
        } else {
            format!("{}:{}", base_url, method)
        }
    }

    /// Determine if we should try next endpoint (fallback logic)
    fn should_try_next_endpoint(status: StatusCode) -> bool {
        status == StatusCode::REQUEST_TIMEOUT
            || status == StatusCode::NOT_FOUND
            || status.is_server_error()
    }

    /// Call v1internal API (Basic Method)
    ///
    /// Initiates a basic network request, supporting multi-endpoint auto-fallback.
    /// [UPDATED] Takes optional account_id for per-account proxy selection.
    pub async fn call_v1_internal(
        &self,
        method: &str,
        access_token: &str,
        body: Value,
        query_string: Option<&str>,
        account_id: Option<&str>, // [NEW] Account ID for proxy selection
    ) -> Result<UpstreamCallResult, String> {
        self.call_v1_internal_with_headers(
            method,
            access_token,
            body,
            query_string,
            std::collections::HashMap::new(),
            account_id,
        )
        .await
    }

    /// [FIX #765] 调用 v1internal API，支持透传额外的 Headers
    /// [ENHANCED] 返回 UpstreamCallResult，包含降级尝试记录，用于 debug 日志
    pub async fn call_v1_internal_with_headers(
        &self,
        method: &str,
        access_token: &str,
        body: Value,
        query_string: Option<&str>,
        extra_headers: std::collections::HashMap<String, String>,
        account_id: Option<&str>, // [NEW] Account ID
    ) -> Result<UpstreamCallResult, String> {
        self.call_v1_internal_with_headers_on_endpoints(
            method,
            access_token,
            body,
            query_string,
            extra_headers,
            account_id,
            &V1_INTERNAL_BASE_URL_FALLBACKS,
            None,
        )
        .await
    }

    async fn call_v1_internal_with_headers_on_endpoints(
        &self,
        method: &str,
        access_token: &str,
        mut body: Value,
        query_string: Option<&str>,
        extra_headers: std::collections::HashMap<String, String>,
        account_id: Option<&str>,
        endpoint_urls: &[&str],
        attempt_budget_path: Option<std::path::PathBuf>,
    ) -> Result<UpstreamCallResult, String> {
        let is_live_test_image_generation = cfg!(feature = "native-image-live-test")
            && method == "generateContent"
            && body.get("requestType").and_then(Value::as_str) == Some("image_gen");
        let image_log_context =
            is_live_test_image_generation.then(|| native_image_log_context(&body));

        if is_live_test_image_generation && endpoint_urls.is_empty() {
            return Err("No upstream endpoint configured; image request refused".to_string());
        }

        // The dedicated live-test build uses one endpoint only. It never falls back,
        // including for network errors, 404s, timeouts, or 5xx responses.
        let selected_endpoints: Vec<&str> = if is_live_test_image_generation {
            endpoint_urls.iter().take(1).copied().collect()
        } else {
            endpoint_urls.to_vec()
        };

        // [DEFENSE] 全局终极防御拦截：净化所有发往上游报文中的损坏/空 inlineData 以及触发 Google WAF 拦截的违规计费元数据，并最终统一对齐官方信封与前缀拓扑
        if let Some(inner) = body.get_mut("request") {
            crate::proxy::mappers::common_utils::sanitize_gemini_payload_inline_data(inner);
            crate::proxy::mappers::prompt_sanitizer::PromptSanitizer::sanitize_gemini_payload(
                inner,
            );
            crate::proxy::mappers::common_utils::ensure_gemini_payload_ends_with_user(inner);
        } else {
            crate::proxy::mappers::common_utils::sanitize_gemini_payload_inline_data(&mut body);
            crate::proxy::mappers::prompt_sanitizer::PromptSanitizer::sanitize_gemini_payload(
                &mut body,
            );
            crate::proxy::mappers::common_utils::ensure_gemini_payload_ends_with_user(&mut body);
        }
        crate::proxy::pipeline::InboundThinkingPipeline::align_official_envelope(&mut body);

        // [NEW] Get client based on account (cached in proxy pool manager)
        let client = self.get_client(account_id).await;

        // 构建 Headers (所有端点复用)
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static("application/json"),
        );
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", access_token))
                .map_err(|e| e.to_string())?,
        );

        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_str(&self.get_user_agent().await).unwrap_or_else(|e| {
                tracing::warn!("Invalid User-Agent header value, using fallback: {}", e);
                header::HeaderValue::from_str(crate::constants::USER_AGENT.as_str())
                    .unwrap_or_else(|_| header::HeaderValue::from_static("antigravity"))
            }),
        );

        // Keep body.project for content requests, but omit the quota-project header.
        let is_content_request = matches!(method, "generateContent" | "streamGenerateContent");
        if !is_content_request {
            if let Some(proj) = body.get("project").and_then(|v| v.as_str()) {
                if !proj.is_empty() && proj != "test-project" && proj != "project-id" {
                    if let Ok(hv) = header::HeaderValue::from_str(proj) {
                        headers.insert("x-goog-user-project", hv);
                    }
                }
            }
        }

        // 注入业务透传 Headers (如 anthropic-beta)
        // 严格过滤客户端特征伪头，确保出站请求头 100% 对齐官方 Antigravity Hub
        for (k, v) in extra_headers {
            let k_lower = k.to_ascii_lowercase();
            if k_lower == "user-agent"
                || k_lower == "x-session-id"
                || k_lower == "x-client-name"
                || k_lower == "x-client-version"
                || k_lower == "x-machine-id"
                || k_lower == "x-vscode-sessionid"
                || k_lower.starts_with("x-jeikcode")
            {
                continue;
            }
            if let Ok(hk) = header::HeaderName::from_bytes(k.as_bytes()) {
                if let Ok(hv) = header::HeaderValue::from_str(&v) {
                    headers.insert(hk, hv);
                }
            }
        }
        if is_content_request {
            headers.remove("x-goog-user-project");
        }

        // [PIPELINE ALIGNMENT] 统一对齐官方上游特权头：所有 Claude 系列模型出站统一注入 anthropic-beta 声明
        let target_model_str = body
            .get("model")
            .and_then(|v| v.as_str())
            .or_else(|| {
                body.get("request")
                    .and_then(|r| r.get("model"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");
        if target_model_str.to_lowercase().contains("claude")
            && !headers.contains_key("anthropic-beta")
        {
            headers.insert(
                header::HeaderName::from_static("anthropic-beta"),
                header::HeaderValue::from_static("claude-code-20250219"),
            );
        }

        // Never emit header values: this map includes the OAuth Authorization value.
        tracing::debug!(
            header_count = headers.len(),
            "Final upstream headers prepared"
        );

        let _ = crate::proxy::monitor::CURRENT_UPSTREAM_CAPTURE.try_with(|holder| {
            let pairs: Vec<(&str, &str)> = headers
                .iter()
                .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.as_str(), s)))
                .collect();
            holder.set_headers_json(crate::proxy::payload_audit::header_pairs_to_redacted_json(
                pairs,
            ));
        });

        let mut has_triggered_downgrade = false;

        // [TEMPORARY FIX #3074] 针对 403 SERVICE_DISABLED 的自动降级重试逻辑
        // 我们包装一层循环，以便在检测到特定错误时移除 Header 并重试
        loop {
            let mut last_err: Option<String> = None;
            let mut fallback_attempts: Vec<FallbackAttemptLog> = Vec::new();
            let mut should_retry_without_header = false;

            // 遍历所有端点，失败时自动切换
            for (idx, base_url) in selected_endpoints.iter().enumerate() {
                let url = Self::build_url(base_url, method, query_string);
                let has_next = idx + 1 < selected_endpoints.len();

                let body_bytes = serde_json::to_vec(&body).map_err(|e| e.to_string())?;

                let mut req_builder = client.post(&url).headers(headers.clone());

                // [FIX] 仅对流式接口 (streamGenerateContent) 使用分块传输仿真
                // 对其他接口 (如 generateContent, loadCodeAssist) 发送正常的固定长度 Body
                // 否则图像生成会因为缺少 Content-Length 而被 Google 服务端拒绝或限流 (429)
                if method == "streamGenerateContent" {
                    let stream_bytes = body_bytes.clone();
                    req_builder = req_builder.body(rquest::Body::wrap_stream(
                        futures::stream::once(async move { Ok::<_, std::io::Error>(stream_bytes) }),
                    ));
                } else {
                    req_builder = req_builder.body(body_bytes.clone());
                }

                if is_live_test_image_generation {
                    req_builder = req_builder.timeout(live_image_request_timeout());
                }

                let mut reserved_attempt = None;
                let mut image_event_db_path = None;

                // Persist a reservation before every actual send. This deliberately
                // counts requests that later fail at the socket/HTTP layer and closes
                // races between callers and attempts across process restarts.
                if is_live_test_image_generation {
                    let reservation = if let Some(path) = attempt_budget_path.clone() {
                        tokio::task::spawn_blocking(move || {
                            crate::proxy::image_attempt_budget::reserve_attempt_at(&path)
                        })
                        .await
                        .map_err(|error| format!("Image attempt budget worker failed: {error}"))?
                    } else {
                        crate::proxy::image_attempt_budget::reserve_current_profile_attempt().await
                    };
                    let attempt = reservation.map_err(|error| error.to_string())?;
                    let evidence_path = match attempt_budget_path.clone() {
                        Some(path) => path,
                        None => crate::proxy::image_attempt_budget::current_profile_budget_path()
                            .map_err(|error| error.to_string())?,
                    };
                    crate::proxy::image_attempt_budget::record_attempt_evidence_at(
                        &evidence_path,
                        attempt,
                        &url,
                        &body,
                    )
                    .map_err(|error| error.to_string())?;
                    if let Some(context) = image_log_context.as_ref() {
                        record_native_image_event_at(
                            &evidence_path,
                            context,
                            "ATTEMPT_RESERVED",
                            Some(attempt),
                            None,
                            None,
                            None,
                        )?;
                    }
                    reserved_attempt = Some(attempt);
                    image_event_db_path = Some(evidence_path);
                }

                let send_started = std::time::Instant::now();
                let mut send_guard = match (
                    image_log_context.as_ref(),
                    image_event_db_path.as_ref(),
                    reserved_attempt,
                ) {
                    (Some(context), Some(path), Some(attempt)) => {
                        record_native_image_event_at(
                            path,
                            context,
                            "UPSTREAM_SEND_STARTED",
                            Some(attempt),
                            None,
                            None,
                            None,
                        )?;
                        Some(NativeImageSendGuard {
                            budget_db_path: path.clone(),
                            context: context.clone(),
                            attempt,
                            finished: false,
                        })
                    }
                    _ => None,
                };
                let response = req_builder.send().await;

                match response {
                    Ok(resp) => {
                        let status = resp.status();
                        if let (Some(context), Some(path), Some(attempt)) = (
                            image_log_context.as_ref(),
                            image_event_db_path.as_ref(),
                            reserved_attempt,
                        ) {
                            let log_result = record_native_image_event_at(
                                path,
                                context,
                                "UPSTREAM_HTTP_RESPONSE",
                                Some(attempt),
                                Some(status.as_u16()),
                                Some(send_started.elapsed().as_millis()),
                                None,
                            );
                            if let Some(guard) = send_guard.as_mut() {
                                guard.finish();
                            }
                            if log_result.is_err() {
                                tracing::error!(
                                    correlation_id = %context.correlation_id,
                                    model = %context.model,
                                    imageSize = %context.image_size,
                                    "native image response event could not be persisted"
                                );
                            }
                        }
                        if status.is_success() {
                            if idx > 0 {
                                tracing::info!(
                                    "✓ Upstream fallback succeeded | Endpoint: {} | Status: {} | Next endpoints available: {}",
                                    base_url,
                                    status,
                                    selected_endpoints.len() - idx - 1
                                );
                            } else {
                                tracing::debug!(
                                    "✓ Upstream request succeeded | Endpoint: {} | Status: {}",
                                    base_url,
                                    status
                                );
                            }
                            return Ok(UpstreamCallResult {
                                response: resp,
                                fallback_attempts,
                                image_correlation_id: image_log_context
                                    .as_ref()
                                    .map(|context| context.correlation_id.clone()),
                            });
                        }

                        if is_live_test_image_generation {
                            // The single network attempt has completed. Return its
                            // response as-is so the image handler cannot rotate accounts.
                            return Ok(UpstreamCallResult {
                                response: resp,
                                fallback_attempts,
                                image_correlation_id: image_log_context
                                    .as_ref()
                                    .map(|context| context.correlation_id.clone()),
                            });
                        }

                        // [NEW] 检测 403 错误 (Issue #3074)
                        // 只要带有项目 Header 且返回 403，我们就尝试降级重试一次
                        if status == StatusCode::FORBIDDEN
                            && !has_triggered_downgrade
                            && headers.contains_key("x-goog-user-project")
                        {
                            tracing::warn!(
                                "Detected 403 Forbidden with project header, retrying WITHOUT x-goog-user-project header (Account: {:?})",
                                account_id
                            );
                            should_retry_without_header = true;
                            break;
                        }

                        // 如果有下一个端点且当前错误可重试，则切换
                        if has_next && Self::should_try_next_endpoint(status) {
                            let err_msg = format!("Upstream {} returned {}", base_url, status);
                            tracing::warn!(
                                "Upstream endpoint returned {} at {} (method={}), trying next endpoint",
                                status,
                                base_url,
                                method
                            );
                            // [NEW] 记录降级尝试
                            fallback_attempts.push(FallbackAttemptLog {
                                endpoint_url: url.clone(),
                                status: Some(status.as_u16()),
                                error: err_msg.clone(),
                            });
                            last_err = Some(err_msg);
                            continue;
                        }

                        // 不可重试的错误或已是最后一个端点，直接返回
                        return Ok(UpstreamCallResult {
                            response: resp,
                            fallback_attempts,
                            image_correlation_id: image_log_context
                                .as_ref()
                                .map(|context| context.correlation_id.clone()),
                        });
                    }
                    Err(e) => {
                        if let (Some(context), Some(path), Some(attempt)) = (
                            image_log_context.as_ref(),
                            image_event_db_path.as_ref(),
                            reserved_attempt,
                        ) {
                            let timed_out = e.is_timeout();
                            let log_result = record_native_image_event_at(
                                path,
                                context,
                                if timed_out {
                                    "UPSTREAM_TIMEOUT"
                                } else {
                                    "UPSTREAM_ERROR"
                                },
                                Some(attempt),
                                None,
                                Some(send_started.elapsed().as_millis()),
                                Some(if timed_out { "timeout" } else { "transport" }),
                            );
                            if let Some(guard) = send_guard.as_mut() {
                                guard.finish();
                            }
                            if log_result.is_err() {
                                tracing::error!(
                                    correlation_id = %context.correlation_id,
                                    model = %context.model,
                                    imageSize = %context.image_size,
                                    "native image failure event could not be persisted"
                                );
                            }
                            let kind = if timed_out {
                                "timed out"
                            } else {
                                "failed at transport"
                            };
                            return Err(format!(
                                "Native image upstream request {kind}; remote processing outcome is unknown"
                            ));
                        }
                        let msg = format!("HTTP request failed at {}: {}", base_url, e);
                        tracing::debug!("{}", msg);
                        if is_live_test_image_generation {
                            return Err(msg);
                        }
                        // [NEW] 记录网络错误的降级尝试
                        fallback_attempts.push(FallbackAttemptLog {
                            endpoint_url: url.clone(),
                            status: None,
                            error: msg.clone(),
                        });
                        last_err = Some(msg);

                        // 如果是最后一个端点，退出循环
                        if !has_next {
                            break;
                        }
                        continue;
                    }
                }
            }

            // 处理降级逻辑
            if should_retry_without_header {
                headers.remove("x-goog-user-project");
                has_triggered_downgrade = true;
                // 重启外层 loop，从第一个端点再次尝试
                continue;
            }

            // 如果没有触发降级且所有端点都尝试过，返回最后的错误
            let final_err = last_err.unwrap_or_else(|| "All endpoints failed".to_string());
            tracing::error!(
                error = %final_err,
                account = ?account_id,
                method = %method,
                "Upstream network request failed across all endpoints (non-server fault)"
            );
            return Err(final_err);
        }
    }

    /// 调用 v1internal API（带 429 重试,支持闭包）
    ///
    /// 带容错和重试的核心请求逻辑
    ///
    /// # Arguments
    /// * `method` - API method (e.g., "generateContent")
    /// * `query_string` - Optional query string (e.g., "?alt=sse")
    /// * `get_credentials` - 闭包，获取凭证（支持账号轮换）
    /// * `build_body` - 闭包，接收 project_id 构建请求体
    /// * `max_attempts` - 最大重试次数
    ///
    /// # Returns
    /// HTTP Response
    // 已移除弃用的重试方法 (call_v1_internal_with_retry)

    // 已移除弃用的辅助方法 (parse_retry_delay)

    // 已移除弃用的辅助方法 (parse_duration_ms)

    /// 获取可用模型列表
    ///
    /// 获取远端模型列表，支持多端点自动 Fallback
    #[allow(dead_code)] // API ready for future model discovery feature
    pub async fn fetch_available_models(
        &self,
        access_token: &str,
        account_id: Option<&str>,
    ) -> Result<Value, String> {
        // 复用 call_v1_internal，然后解析 JSON
        let result = self
            .call_v1_internal(
                "fetchAvailableModels",
                access_token,
                serde_json::json!({}),
                None,
                account_id,
            )
            .await?;
        let json: Value = result
            .response
            .json()
            .await
            .map_err(|e| format!("Parse json failed: {}", e))?;
        Ok(json)
    }

    /// 辅助型 v1internal 调用（非用户请求路径，例如后台上下文摘要）。
    ///
    /// 为什么需要它：这类调用点位于 mapper / 辅助函数里，历史上拿不到 `AppState.upstream`
    /// 便手写 URL —— 结果形状与 host 双双漂移
    /// （`{host}/v1internal/projects/{p}/locations/global/models/{m}:generateContent`
    /// 在 daily / sandbox / prod 三个 host 上一律返回 HTML 404，导致该功能从未成功过）。
    ///
    /// 这里复用与主请求路径**完全相同**的四个来源，避免再次漂移：
    /// - 客户端 `self.get_client(account_id)` —— 同一套按账号代理池选择与 `client_cache`，
    ///   并随上游代理热更新一并生效（见 `rebuild_default_client` / `clear_client_cache`）。
    ///   账号绑定专属代理时辅助请求同样走它，不会从真实 IP 泄漏出去。
    /// - 端点顺序 `V1_INTERNAL_BASE_URL_FALLBACKS`（Daily → Sandbox → Prod）
    /// - URL 形状 `Self::build_url`（`{base}:{method}`，模型名放在 body 里）
    /// - 回退判定 `Self::should_try_next_endpoint`（408 / 404 / 5xx 才换端点）
    ///
    /// 与 `call_v1_internal` 的差异（有意为之）：不注入请求级 Header
    /// （`x-vscode-sessionid` 等）。超时由调用方用 `timeout_secs` 指定 ——
    /// 客户端默认 600s，对摘要过长，故显式收紧。
    pub async fn call_v1_internal_auxiliary(
        &self,
        method: &str,
        access_token: &str,
        mut body: Value,
        account_id: Option<&str>,
        timeout_secs: u64,
    ) -> Result<Value, String> {
        crate::proxy::pipeline::InboundThinkingPipeline::align_official_envelope(&mut body);
        let client = self.get_client(account_id).await;
        let mut last_error = String::new();

        for base_url in V1_INTERNAL_BASE_URL_FALLBACKS.iter() {
            let url = Self::build_url(base_url, method, None);

            let response = match client
                .post(&url)
                .header("Authorization", format!("Bearer {}", access_token))
                .header("Content-Type", "application/json")
                .timeout(Duration::from_secs(timeout_secs))
                .json(&body)
                .send()
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    tracing::warn!(
                        endpoint = %url,
                        error = %e,
                        "Auxiliary v1internal request failed, trying next endpoint"
                    );
                    last_error = format!("request to {} failed: {}", url, e);
                    continue;
                }
            };

            let status = response.status();
            if !status.is_success() {
                let text = response.text().await.unwrap_or_default();
                last_error = format!("{} returned {}: {}", url, status, text);

                // 与主请求路径同一判定：仅 408 / 404 / 5xx 换端点；
                // 其余状态（如 400）说明请求本身有问题，直接终止，不做三倍重试。
                if !Self::should_try_next_endpoint(status) {
                    tracing::error!(
                        error = %last_error,
                        "Auxiliary v1internal request failed with non-retryable status (non-server fault)"
                    );
                    return Err(last_error);
                }
                tracing::warn!(
                    endpoint = %url,
                    status = %status,
                    "Auxiliary v1internal request returned retryable status, trying next endpoint"
                );
                continue;
            }

            return response
                .json()
                .await
                .map_err(|e| format!("failed to parse response from {}: {}", url, e));
        }

        let final_err = if last_error.is_empty() {
            "no v1internal endpoint available".to_string()
        } else {
            last_error
        };
        tracing::error!(
            error = %final_err,
            "Auxiliary v1internal request failed across all endpoints (non-server fault)"
        );
        Err(final_err)
    }
}

/// 派生确定性 UUID 格式的客户端窗口 Session ID (RFC 4122 v4 格式)
fn derive_session_uuid(seed: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"antigravity-session-v1:");
    hasher.update(seed.as_bytes());
    let hash = hasher.finalize();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hash[0], hash[1], hash[2], hash[3],
        hash[4], hash[5],
        hash[6] & 0x0f, hash[7],
        (hash[8] & 0x3f) | 0x80, hash[9],
        hash[10], hash[11], hash[12], hash[13], hash[14], hash[15]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_url() {
        let base_url = "https://cloudcode-pa.googleapis.com/v1internal";

        let url1 = UpstreamClient::build_url(base_url, "generateContent", None);
        assert_eq!(
            url1,
            "https://cloudcode-pa.googleapis.com/v1internal:generateContent"
        );

        let url2 = UpstreamClient::build_url(base_url, "streamGenerateContent", Some("alt=sse"));
        assert_eq!(
            url2,
            "https://cloudcode-pa.googleapis.com/v1internal:streamGenerateContent?alt=sse"
        );
    }
}

#[cfg(all(test, feature = "native-image-live-test"))]
mod native_image_live_test_mock_tests {
    use super::*;
    use axum::{routing::any, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn feature_build_disables_rquests_hidden_http2_retries() {
        assert_eq!(configured_http2_retry_count(), 0);
    }

    async fn start_mock_upstream_response(
        status: StatusCode,
        body: &'static str,
        delay: Duration,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let handler_hits = hits.clone();
        let app = Router::new().fallback(any(move || {
            let hits = handler_hits.clone();
            let body = body.to_string();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                (status, body)
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            format!("http://{addr}/v1internal/models/mock"),
            hits,
            server,
        )
    }

    async fn start_mock_upstream() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        start_mock_upstream_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "mock upstream failure",
            Duration::ZERO,
        )
        .await
    }

    fn image_generation_body() -> Value {
        serde_json::json!({
            "model": "mock-image-model",
            "requestType": "image_gen",
            "request": {
                "contents": [{"role": "user", "parts": [{"text": "DO_NOT_LOG_PROMPT"}]}],
                "generationConfig": {
                    "candidateCount": 1,
                    "imageConfig": {"imageSize": "2K", "aspectRatio": "1:1"}
                }
            }
        })
    }

    fn fresh_budget_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "native-image-mock-client-{}.sqlite3",
            uuid::Uuid::new_v4()
        ))
    }

    fn read_events(budget_path: &std::path::Path) -> (String, Vec<Value>) {
        let stem = budget_path.file_stem().unwrap().to_string_lossy();
        let prefix = format!("{stem}.events-");
        let event_path = std::fs::read_dir(budget_path.parent().unwrap())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .starts_with(&prefix)
            })
            .expect("expected native image event JSONL");
        let text = std::fs::read_to_string(&event_path).unwrap();
        let events = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        (text, events)
    }

    fn cleanup_budget_files(budget_path: &std::path::Path) {
        let stem = budget_path.file_stem().unwrap().to_string_lossy();
        let prefix = format!("{stem}.events-");
        let evidence_prefix = format!("{stem}.attempt-");
        if let Ok(entries) = std::fs::read_dir(budget_path.parent().unwrap()) {
            for entry in entries.flatten() {
                let path = entry.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name.starts_with(&prefix) || name.starts_with(&evidence_prefix) {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = budget_path.as_os_str().to_os_string();
            sidecar.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(sidecar));
        }
        let _ = std::fs::remove_file(budget_path);
    }

    #[tokio::test]
    async fn successful_mock_logs_correlated_states_without_sensitive_payload() {
        let (mock_url, hits, server) = start_mock_upstream_response(
            StatusCode::OK,
            r#"{"response":{"candidates":[],"test_b64":"MOCK_BASE64_MUST_NOT_APPEAR"}}"#,
            Duration::ZERO,
        )
        .await;
        let budget_path = fresh_budget_path();
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[&mock_url],
                Some(budget_path.clone()),
            )
            .await
            .unwrap();

        assert_eq!(result.response.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let correlation_id = result.image_correlation_id.unwrap();
        let _: Value = result.response.json().await.unwrap();
        record_native_image_response_saved_at(
            &budget_path,
            &correlation_id,
            "mock-image-model",
            "2K",
        )
        .unwrap();
        let (log_text, events) = read_events(&budget_path);
        let event_names: Vec<&str> = events
            .iter()
            .map(|event| event["event"].as_str().unwrap())
            .collect();
        assert_eq!(
            event_names,
            [
                "ATTEMPT_RESERVED",
                "UPSTREAM_SEND_STARTED",
                "UPSTREAM_HTTP_RESPONSE",
                "RESPONSE_SAVED"
            ]
        );
        for event in &events {
            assert_eq!(event["correlation_id"], correlation_id);
            assert_eq!(event["model"], "mock-image-model");
            assert_eq!(event["imageSize"], "2K");
            assert!(event["timestamp_utc"].as_str().is_some());
        }
        assert_eq!(events[2]["http_status"], 200);
        assert!(events[2]["elapsed_ms"].as_u64().is_some());
        for forbidden in [
            "mock-only-token",
            "DO_NOT_LOG_PROMPT",
            "MOCK_BASE64_MUST_NOT_APPEAR",
        ] {
            assert!(!log_text.contains(forbidden));
        }
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );

        server.abort();
        cleanup_budget_files(&budget_path);
    }

    #[test]
    fn client_wait_budget_exceeds_upstream_deadline() {
        assert!(LIVE_IMAGE_CLIENT_TIMEOUT_SECS > LIVE_IMAGE_UPSTREAM_TIMEOUT_SECS);
    }

    #[tokio::test]
    async fn failed_mock_attempt_is_counted_and_does_not_fallback() {
        let (first_url, first_hits, first_server) = start_mock_upstream().await;
        let (fallback_url, fallback_hits, fallback_server) = start_mock_upstream().await;
        let budget_path = fresh_budget_path();
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[&first_url, &fallback_url],
                Some(budget_path.clone()),
            )
            .await
            .unwrap();

        assert_eq!(result.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(result.fallback_attempts.is_empty());
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );
        let (log_text, events) = read_events(&budget_path);
        assert_eq!(events[2]["event"], "UPSTREAM_HTTP_RESPONSE");
        assert_eq!(events[2]["http_status"], 500);
        assert!(!log_text.contains("mock-only-token"));
        assert!(!log_text.contains("DO_NOT_LOG_PROMPT"));

        first_server.abort();
        fallback_server.abort();
        cleanup_budget_files(&budget_path);
    }

    #[tokio::test]
    async fn network_failure_is_reserved_and_does_not_try_another_host() {
        let (fallback_url, fallback_hits, fallback_server) = start_mock_upstream().await;
        let dead_first_url = "http://127.0.0.1:1/v1internal/models/unreachable";
        let budget_path = fresh_budget_path();
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[dead_first_url, &fallback_url],
                Some(budget_path.clone()),
            )
            .await;

        assert!(result.is_err());
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );
        let (log_text, events) = read_events(&budget_path);
        assert!(matches!(
            events.last().unwrap()["event"].as_str(),
            Some("UPSTREAM_ERROR" | "UPSTREAM_TIMEOUT")
        ));
        assert!(!log_text.contains("mock-only-token"));

        fallback_server.abort();
        cleanup_budget_files(&budget_path);
    }

    #[tokio::test]
    async fn upstream_timeout_is_logged_and_does_not_fallback() {
        let (first_url, first_hits, first_server) =
            start_mock_upstream_response(StatusCode::OK, "mock response", Duration::from_secs(2))
                .await;
        let (fallback_url, fallback_hits, fallback_server) = start_mock_upstream().await;
        let budget_path = fresh_budget_path();
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[&first_url, &fallback_url],
                Some(budget_path.clone()),
            )
            .await;

        let error = result.err().unwrap();
        assert!(error.contains("remote processing outcome is unknown"));
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 0);
        let (_log_text, events) = read_events(&budget_path);
        assert_eq!(events.last().unwrap()["event"], "UPSTREAM_TIMEOUT");
        assert_eq!(events.last().unwrap()["error_kind"], "timeout");
        assert!(events.last().unwrap()["elapsed_ms"].as_u64().is_some());
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );

        first_server.abort();
        fallback_server.abort();
        cleanup_budget_files(&budget_path);
    }

    #[tokio::test]
    async fn cancelled_inflight_send_is_logged_without_claiming_remote_cancellation() {
        let (mock_url, hits, server) =
            start_mock_upstream_response(StatusCode::OK, "mock response", Duration::from_secs(5))
                .await;
        let budget_path = fresh_budget_path();
        let client = Arc::new(UpstreamClient::new(None, None));
        let task_client = client.clone();
        let task_url = mock_url.clone();
        let task_budget = budget_path.clone();
        let task = tokio::spawn(async move {
            task_client
                .call_v1_internal_with_headers_on_endpoints(
                    "generateContent",
                    "mock-only-token",
                    image_generation_body(),
                    None,
                    Default::default(),
                    None,
                    &[&task_url],
                    Some(task_budget),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while hits.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;

        let (log_text, events) = read_events(&budget_path);
        assert_eq!(events.last().unwrap()["event"], "UPSTREAM_INTERRUPTED");
        assert_eq!(
            events.last().unwrap()["error_kind"],
            "local_future_cancelled"
        );
        assert!(!log_text.contains("mock-only-token"));
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );

        server.abort();
        cleanup_budget_files(&budget_path);
    }

    #[tokio::test]
    async fn redirect_response_does_not_issue_a_hidden_second_request() {
        let (redirect_target_url, target_hits, target_server) = start_mock_upstream().await;
        let redirect_hits = Arc::new(AtomicUsize::new(0));
        let handler_hits = redirect_hits.clone();
        let redirect_app = Router::new().fallback(any(move || {
            let hits = handler_hits.clone();
            let target = redirect_target_url.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [(axum::http::header::LOCATION, target)],
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = listener.local_addr().unwrap();
        let redirect_server = tokio::spawn(async move {
            axum::serve(listener, redirect_app).await.unwrap();
        });
        let redirect_url = format!("http://{redirect_addr}/v1internal/models/redirect");
        let budget_path = fresh_budget_path();
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[&redirect_url],
                Some(budget_path.clone()),
            )
            .await
            .unwrap();

        assert_eq!(result.response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(redirect_hits.load(Ordering::SeqCst), 1);
        assert_eq!(target_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            1
        );

        redirect_server.abort();
        target_server.abort();
        let _ = std::fs::remove_file(budget_path);
    }

    #[tokio::test]
    async fn repeated_requests_and_new_client_instances_share_the_five_attempt_cap() {
        let (mock_url, hits, server) = start_mock_upstream().await;
        let budget_path = fresh_budget_path();

        // A fresh UpstreamClient per iteration models proxy process/client restarts.
        for _ in 0..5 {
            let client = UpstreamClient::new(None, None);
            let result = client
                .call_v1_internal_with_headers_on_endpoints(
                    "generateContent",
                    "mock-only-token",
                    image_generation_body(),
                    None,
                    Default::default(),
                    None,
                    &[&mock_url],
                    Some(budget_path.clone()),
                )
                .await
                .unwrap();
            assert_eq!(result.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        }

        let sixth_client = UpstreamClient::new(None, None);
        let sixth = sixth_client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                image_generation_body(),
                None,
                Default::default(),
                None,
                &[&mock_url],
                Some(budget_path.clone()),
            )
            .await;
        assert!(sixth.err().unwrap().contains("attempt budget exhausted"));
        assert_eq!(hits.load(Ordering::SeqCst), 5);
        assert_eq!(
            crate::proxy::image_attempt_budget::current_usage_at(&budget_path).unwrap(),
            5
        );

        server.abort();
        let _ = std::fs::remove_file(budget_path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_mock_requests_cannot_send_more_than_five_attempts() {
        let (mock_url, hits, server) = start_mock_upstream().await;
        let budget_path = fresh_budget_path();
        let client = Arc::new(UpstreamClient::new(None, None));
        let mut jobs = Vec::new();
        for _ in 0..24 {
            let client = client.clone();
            let mock_url = mock_url.clone();
            let budget_path = budget_path.clone();
            jobs.push(tokio::spawn(async move {
                client
                    .call_v1_internal_with_headers_on_endpoints(
                        "generateContent",
                        "mock-only-token",
                        image_generation_body(),
                        None,
                        Default::default(),
                        None,
                        &[&mock_url],
                        Some(budget_path),
                    )
                    .await
            }));
        }

        let mut sent = 0;
        let mut rejected = 0;
        for job in jobs {
            match job.await.unwrap() {
                Ok(response) => {
                    assert_eq!(
                        response.response.status(),
                        StatusCode::INTERNAL_SERVER_ERROR
                    );
                    sent += 1;
                }
                Err(message) if message.contains("attempt budget exhausted") => rejected += 1,
                Err(message) => panic!("unexpected concurrent mock error: {message}"),
            }
        }
        assert_eq!(sent, 5);
        assert_eq!(rejected, 19);
        assert_eq!(hits.load(Ordering::SeqCst), 5);

        server.abort();
        let _ = std::fs::remove_file(budget_path);
    }

    #[tokio::test]
    async fn non_image_generation_keeps_the_existing_endpoint_fallback_behavior() {
        let (first_url, first_hits, first_server) = start_mock_upstream().await;
        let (second_url, second_hits, second_server) = start_mock_upstream().await;
        let client = UpstreamClient::new(None, None);
        let result = client
            .call_v1_internal_with_headers_on_endpoints(
                "generateContent",
                "mock-only-token",
                serde_json::json!({"requestType": "code", "request": {"contents": []}}),
                None,
                Default::default(),
                None,
                &[&first_url, &second_url],
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(result.fallback_attempts.len(), 1);
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);

        first_server.abort();
        second_server.abort();
    }
}
