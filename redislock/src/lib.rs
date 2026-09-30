use redis::aio::MultiplexedConnection;
use std::{
    fmt::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{self, Duration},
};

/// lua scripts
const LUA_OBTAIN: &str = include_str!("../script/obtain.lua");
const LUA_PTTL: &str = include_str!("../script/pttl.lua");
const LUA_REFRESH: &str = include_str!("../script/refresh.lua");
const LUA_RELEASE: &str = include_str!("../script/release.lua");

/// Client wraps a redis client
pub struct Client {
    pub cli: MultiplexedConnection,
    pub token: Arc<Mutex<Vec<u8>>>, //
}

/// Lock represents an obtained, distributed lock.
pub struct Lock {
    pub cli: Box<Client>,
    pub keys: Vec<String>,
    pub value: String,
    pub token_len: i32,
    pub fence_token: i64,
}

/// Options describe the options for the lock
pub struct Options {
    /// RetryStrategy allows to customise the lock retry strategy.
    /// Default: do not retry
    pub retry_strategy: RetryStrategy,

    /// Metadata string.
    pub metadata: String,

    /// Token is a unique value that is used to identify the lock. By default, a random tokens are generated. Use this
    /// option to provide a custom token instead.
    pub token: String,

    /// FenceKey enables a fencing token, minted at this key on each new
    /// acquisition and returned by Lock.FenceToken. On Redis Cluster it must hash
    /// to the same slot as the lock key(s).
    /// Default: empty, no fencing.
    pub fenceKey: String,
}

/// Client methods
impl Client {
    /// new a Client instance
    pub fn new(cli: MultiplexedConnection) -> Self {
        Self {
            cli,
            token: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// obtain tries to obtain a new lock using a key with the given TTL.
    /// May return ErrorKind::NotObtained if not successful.
    async fn obtain(
        &self,
        key: &str,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock, ErrorKind> {
        Err(ErrorKind::LockNotHeld)
    }

    /// obtain_multi tries to obtain a new lock using a key with the given TTL.
    /// May return ErrorKind::NotObtained if not successful.
    async fn obtain_multi(
        &self,
        key: &str,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock, ErrorKind> {
        Err(ErrorKind::LockNotHeld)
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn redis_conn() {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        redis::cmd("SET")
            .arg(&["foo", "bar"])
            .exec_async(&mut conn)
            .await
            .unwrap();
        let result: Vec<String> = redis::cmd("GET")
            .arg(&["foo"])
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(result, ["bar"]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redis_script() {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let script = redis::Script::new(
            r"
            return tonumber(ARGV[1]) + tonumber(ARGV[2]);
        ",
        );
        let result: Result<i32, _> = script.arg(&[1, 2]).invoke_async(&mut conn).await.unwrap();
        assert_eq!(result, Ok(3));
    }

    // #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[tokio::test(flavor = "current_thread")]
    async fn redis_script_obtain() {
        let client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let script = redis::Script::new(LUA_OBTAIN);
        match script
            .key("lock:foo")
            .arg(("abc123", 1, 1, 0))
            .invoke_async::<String>(&mut conn)
            .await
        {
            Ok(result) => {
                println!("result = {result}");
            }
            Err(e) => {
                println!("error = {e}")
            }
        }
    }
}

/// errors
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ErrorKind {
    NotObtained,
    LockNotHeld,
    RetryBaseStrategyNone,
    RetryMaxNone,
    RetryMinNone,
}

///
impl ErrorKind {
    const fn as_str(&self) -> &'static str {
        use ErrorKind::*;
        match *self {
            // tidy-alphabetical-start
            LockNotHeld => "redislock: lock not held",
            NotObtained => "redislock: not obtained",
            RetryBaseStrategyNone => "retry: none base strategy",
            RetryMaxNone => "retry: none max turns",
            RetryMinNone => "retry: none min turns",
        }
    }
}

/// retry

pub struct ExponentialBackoff {
    cnt: AtomicU64,    // count
    pub min: Duration, // min duration
    pub max: Duration, // max duration
}

impl ExponentialBackoff {
    pub fn new(min: Duration, max: Duration) -> ExponentialBackoff {
        ExponentialBackoff {
            cnt: AtomicU64::new(0),
            min,
            max,
        }
    }
}

pub struct LimitedRetry {
    cnt: AtomicU64,               // count
    pub max: u64,                 // max count
    pub s: Option<RetryStrategy>, // base retry strategy
}

impl LimitedRetry {
    pub fn new(s: RetryStrategy, max: u64) -> LimitedRetry {
        LimitedRetry {
            cnt: AtomicU64::new(0),
            max,
            s: Some(s),
        }
    }
}

/// RetryStrategy allows to customise the lock retry strategy.
pub enum RetryStrategy {
    NoRetry,
    LinearBackoff(Duration),
    ExponentialBackoff(ExponentialBackoff),
    LimitedRetry(Box<LimitedRetry>),
}

impl RetryStrategy {
    /// next_back_off returns the next backoff duration.
    pub fn next_back_off(&self) -> Result<Duration, ErrorKind> {
        use RetryStrategy::*;
        match self {
            NoRetry => Ok(Duration::ZERO),
            LinearBackoff(dur) => Ok(dur.clone()),
            ExponentialBackoff(backoff) => {
                let new = backoff.cnt.fetch_add(1, Ordering::Relaxed) + 1;
                let mut ms = 2 << 25;
                if new < 25 {
                    ms = 2 << new;
                }
                let d = Duration::from_millis(ms);
                if d < backoff.min {
                    Ok(backoff.min)
                } else if !backoff.max.is_zero() && d > backoff.max {
                    Ok(backoff.max)
                } else {
                    Ok(d)
                }
            }
            LimitedRetry(retry) => {
                let Some(s) = &retry.s else {
                    return Err(ErrorKind::RetryBaseStrategyNone);
                };
                let new = retry.cnt.fetch_add(1, Ordering::Relaxed) + 1;
                if new > retry.max {
                    Ok(Duration::ZERO)
                } else {
                    s.next_back_off()
                }
            }
        }
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;

    // retry strategies

    #[test]
    fn no_retry() {
        let retry = RetryStrategy::NoRetry;
        for exp in [Duration::ZERO, Duration::ZERO, Duration::ZERO] {
            assert_eq!(retry.next_back_off(), Ok(exp));
        }
    }

    #[test]
    fn linear_backoff() {
        let retry = RetryStrategy::LinearBackoff(Duration::from_secs(1));
        for exp in [
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
        ] {
            assert_eq!(retry.next_back_off(), Ok(exp));
        }
    }

    #[test]
    fn exponential_backoff() {
        let retry = RetryStrategy::ExponentialBackoff(ExponentialBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(300),
        ));
        for exp in [
            Duration::from_millis(10),
            Duration::from_millis(10),
            Duration::from_millis(16),
            Duration::from_millis(32),
            Duration::from_millis(64),
            Duration::from_millis(128),
            Duration::from_millis(256),
            Duration::from_millis(300),
            Duration::from_millis(300),
        ] {
            assert_eq!(retry.next_back_off(), Ok(exp));
        }
    }

    #[test]
    fn limited_retry() {
        let retry = RetryStrategy::LimitedRetry(Box::new(LimitedRetry::new(
            RetryStrategy::LinearBackoff(Duration::from_secs(1)),
            2,
        )));
        for exp in [
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(0),
        ] {
            assert_eq!(retry.next_back_off(), Ok(exp));
        }
    }

    #[test]
    fn limited_retry_concurrent() {}
}
