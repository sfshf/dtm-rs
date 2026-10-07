use base64::prelude::*;
use core::fmt;
use redis::aio::MultiplexedConnection;
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// lua scripts
const LUA_OBTAIN: &str = include_str!("../script/obtain.lua");
const LUA_PTTL: &str = include_str!("../script/pttl.lua");
const LUA_REFRESH: &str = include_str!("../script/refresh.lua");
const LUA_RELEASE: &str = include_str!("../script/release.lua");

/// Client wraps a redis client
pub struct Client {
    cli: MultiplexedConnection,
    token: Arc<Mutex<Vec<u8>>>, //
}

/// Options describe the options for the lock
pub struct Options {
    /// RetryStrategy allows to customise the lock retry strategy.
    /// Default: do not retry
    pub retry_strategy: RetryStrategy,

    /// Metadata string.
    pub meta_data: String,

    /// Token is a unique value that is used to identify the lock. By default, a random tokens are generated. Use this
    /// option to provide a custom token instead.
    pub token: String,

    /// FenceKey enables a fencing token, minted at this key on each new
    /// acquisition and returned by Lock.FenceToken. On Redis Cluster it must hash
    /// to the same slot as the lock key(s).
    /// Default: empty, no fencing.
    pub fence_key: String,
}

/// Client methods
impl Client {
    /// new a Client instance
    pub fn new<'a>(cli: MultiplexedConnection) -> Self {
        Self {
            cli,
            token: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// obtain tries to obtain a new lock using a key with the given TTL.
    /// May return ErrorKind::NotObtained if not successful.
    pub async fn obtain<'a>(
        &'a mut self,
        key: String,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock<'a>, Box<dyn Error>> {
        self.obtain_multi(Vec::from([key]), ttl, opt).await
    }

    /// obtain_multi tries to obtain a new lock using a key with the given TTL.
    /// If any of requested key are already locked, no additional keys are
    /// locked and ErrorKind::NotObtained is returned.
    /// May return ErrorKind::NotObtained if not successful.
    pub async fn obtain_multi<'a>(
        &'a mut self,
        mut keys: Vec<String>,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock<'a>, Box<dyn Error>> {
        let mut opt = opt.unwrap_or(Options {
            retry_strategy: RetryStrategy::NoRetry,
            meta_data: "".to_string(),
            token: "".to_string(),
            fence_key: "".to_string(),
        });
        if opt.token.is_empty() {
            opt.token = self.random_token()?;
        }
        let token_len = opt.token.len();
        let mut run_keys = keys.clone();
        // value is the token + metadata, which is stored in redis as the value of the lock key.
        let value = opt.token.clone() + &opt.meta_data;

        // obtain
        let fence = match opt.fence_key.is_empty() {
            true => 0,
            false => {
                run_keys.push(opt.fence_key);
                1
            }
        };
        let script = redis::Script::new(LUA_OBTAIN);
        loop {
            match script
                .key(&run_keys)
                .arg((&value, token_len, ttl.as_millis(), fence))
                .invoke_async::<i64>(&mut self.cli)
                .await
            {
                Ok(fence_token) => {
                    return Ok(Lock {
                        cli: self,
                        keys,
                        value,
                        token_len,
                        fence_token,
                    });
                }
                Err(e) => {
                    match e.kind() {
                        redis::ErrorKind::Server(_) => {
                            // any non-nil error from obtain is terminal (transient redis
                            // errors are unlikely to clear within a lock TTL and retrying a
                            // broken server is futile).
                            return Err(e.to_string().into());
                        }
                        _ => {
                            // retry
                            let backoff = opt.retry_strategy.next_back_off()?;
                            if backoff.is_zero() {
                                return Err(ErrorKind::NotObtained.into());
                            }
                            tokio::time::sleep(backoff).await;
                        }
                    }
                }
            }
        }
    }

    /// random_token generates a random token for the lock.
    fn random_token(&self) -> Result<String, Box<dyn Error>> {
        let mut token = self.token.lock().map_err(|_| "token mutex poisoned")?;
        rand::fill(&mut *token);
        Ok(BASE64_URL_SAFE.encode(token.as_slice()))
    }
}

/// Lock represents an obtained, distributed lock.
pub struct Lock<'a> {
    pub cli: &'a mut Client,
    pub keys: Vec<String>,
    pub value: String,
    pub token_len: usize,
    pub fence_token: i64,
}

impl<'a> Lock<'a> {
    /// key returns the redis key used by the lock.
    /// If the lock hold multiple key, only the first is returned.
    pub fn key(&self) -> &str {
        self.keys.first().map(|s| s.as_str()).unwrap_or("")
    }

    /// keys returns the redis keys used by the lock.
    pub fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }

    /// token returns the token value set by the lock.
    pub fn token(&self) -> &str {
        &self.value[0..self.token_len]
    }

    /// fence_token returns the lock's fencing token, or 0 if it was obtained without Options.fence_key.
    /// Tokens start at 1, so 0 always means unfenced.
    /// The fencing token is guaranteed to be monotonically increasing for each new lock acquisition,
    /// so it can be used to order operations on the resource protected by the lock.
    pub fn fence_token(&self) -> i64 {
        self.fence_token
    }

    /// meta_data returns the metadata of the lock.
    pub fn meta_data(&self) -> &str {
        &self.value[self.token_len..]
    }

    /// ttl returns the remaining time-to-live. Returns 0 if the lock has expired.
    /// In case lock is holding multiple keys, ttl returns the min ttl among those
    pub fn ttl(&self) -> Result<Duration, Box<dyn Error>> {
        unimplemented!()
    }

    /// refresh extends the lock with a new ttl.
    /// May retrun ErrorKind::NotObtained if refresh is unsuccessful.
    pub fn refresh(&self, ttl: Duration, opt: Option<Options>) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    /// release manually releases the lock.
    /// May return ErrorKind::LockNotHeld if the lock is not held by the caller.
    pub fn release(&self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
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

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Error for ErrorKind {}

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
