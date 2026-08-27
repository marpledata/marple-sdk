use reqwest::{
    Method, RequestBuilder, Response, StatusCode,
    header::{HeaderMap, RETRY_AFTER},
};
use std::time::{Duration, SystemTime};

/// urllib3 / Python SDK cap for exponential backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(120);

/// Retry policy matching `marple.utils.DBClient.API_RETRY`.
pub(crate) const API_RETRY: RetryPolicy = RetryPolicy {
    total: 5,
    connect: 5,
    read: 2,
    status: 2,
    backoff_factor: 0.5,
    status_forcelist: &[429, 502, 503, 504],
    allowed_methods: &[Method::GET, Method::HEAD, Method::OPTIONS, Method::DELETE],
};

/// Retry policy matching `marple.utils.DBClient.STORAGE_RETRY`.
pub(crate) const STORAGE_RETRY: RetryPolicy = RetryPolicy {
    total: 5,
    connect: 5,
    read: 5,
    status: 5,
    backoff_factor: 0.5,
    status_forcelist: &[429, 500, 502, 503, 504],
    allowed_methods: &[Method::PUT, Method::GET, Method::HEAD],
};

/// urllib3-style retry counters and method/status filters.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    total: u32,
    connect: u32,
    read: u32,
    status: u32,
    backoff_factor: f64,
    status_forcelist: &'static [u16],
    allowed_methods: &'static [Method],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportKind {
    Connect,
    Read,
    Other,
}

impl RetryPolicy {
    fn method_allowed(self, method: &Method) -> bool {
        self.allowed_methods.iter().any(|allowed| allowed == method)
    }

    fn retry_status(self, method: &Method, status: StatusCode) -> bool {
        self.method_allowed(method) && self.status_forcelist.contains(&status.as_u16())
    }

    /// urllib3 `Retry.get_backoff_time`: first retry is immediate.
    fn backoff(self, consecutive_errors: u32) -> Duration {
        if consecutive_errors <= 1 {
            return Duration::ZERO;
        }
        let exp = consecutive_errors.saturating_sub(1).min(16);
        let secs = self.backoff_factor * f64::from(1u32 << exp);
        Duration::from_secs_f64(secs).min(BACKOFF_MAX)
    }

    fn delay(self, consecutive_errors: u32, headers: &HeaderMap) -> Duration {
        retry_after_delay(headers).unwrap_or_else(|| self.backoff(consecutive_errors))
    }
}

/// Sends `request`, retrying cloneable requests with the Python SDK policy.
///
/// Streamed bodies cannot be cloned, so those requests are attempted once.
pub(crate) async fn send_with_retry(
    request: RequestBuilder,
    method: &Method,
    policy: &RetryPolicy,
) -> reqwest::Result<Response> {
    let mut connect = 0;
    let mut read = 0;
    let mut status = 0;
    let mut total = 0;

    loop {
        let Some(pending) = request.try_clone() else {
            return request.send().await;
        };

        match pending.send().await {
            Ok(response) => {
                if policy.retry_status(method, response.status())
                    && total < policy.total
                    && status < policy.status
                {
                    let delay = policy.delay(total + 1, response.headers());
                    drop(response);
                    status += 1;
                    total += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Ok(response);
            }
            Err(error) => {
                let kind = transport_kind(&error);
                let (count, cap, method_ok) = match kind {
                    TransportKind::Connect => (connect, policy.connect, true),
                    TransportKind::Read => (read, policy.read, policy.method_allowed(method)),
                    TransportKind::Other => return Err(error),
                };
                if total < policy.total && count < cap && method_ok {
                    let delay = policy.delay(total + 1, &HeaderMap::new());
                    match kind {
                        TransportKind::Connect => connect += 1,
                        TransportKind::Read => read += 1,
                        TransportKind::Other => unreachable!(),
                    }
                    total += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

fn transport_kind(error: &reqwest::Error) -> TransportKind {
    if error.is_connect() {
        TransportKind::Connect
    } else if error.is_timeout() || error.is_body() {
        TransportKind::Read
    } else {
        TransportKind::Other
    }
}

fn retry_after_delay(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(secs) = value.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = httpdate::parse_http_date(value).ok()?;
    when.duration_since(SystemTime::now()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn api_retries_get_on_force_list_but_not_post() {
        assert!(API_RETRY.retry_status(&Method::GET, StatusCode::TOO_MANY_REQUESTS));
        assert!(API_RETRY.retry_status(&Method::GET, StatusCode::BAD_GATEWAY));
        assert!(!API_RETRY.retry_status(&Method::GET, StatusCode::INTERNAL_SERVER_ERROR));
        assert!(!API_RETRY.retry_status(&Method::POST, StatusCode::SERVICE_UNAVAILABLE));
        assert!(!API_RETRY.retry_status(&Method::PATCH, StatusCode::SERVICE_UNAVAILABLE));
        assert!(API_RETRY.method_allowed(&Method::DELETE));
        assert!(!API_RETRY.method_allowed(&Method::PUT));
    }

    #[test]
    fn storage_retries_put_including_500() {
        assert!(STORAGE_RETRY.retry_status(&Method::PUT, StatusCode::INTERNAL_SERVER_ERROR));
        assert!(STORAGE_RETRY.retry_status(&Method::GET, StatusCode::TOO_MANY_REQUESTS));
        assert!(!STORAGE_RETRY.retry_status(&Method::POST, StatusCode::BAD_GATEWAY));
    }

    #[test]
    fn urllib3_backoff_is_zero_on_first_retry() {
        assert_eq!(API_RETRY.backoff(0), Duration::ZERO);
        assert_eq!(API_RETRY.backoff(1), Duration::ZERO);
        assert_eq!(API_RETRY.backoff(2), Duration::from_secs(1));
        assert_eq!(API_RETRY.backoff(3), Duration::from_secs(2));
        assert_eq!(API_RETRY.backoff(4), Duration::from_secs(4));
    }

    #[test]
    fn retry_after_seconds_overrides_backoff() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(API_RETRY.delay(1, &headers), Duration::from_secs(7));
    }
}
