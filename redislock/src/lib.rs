use base64::{engine::general_purpose::URL_SAFE_PAD_INDIFFERENT, prelude::*};
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
use tokio::time;

/// lua scripts
const LUA_OBTAIN: &str = include_str!("../script/obtain.lua");
const LUA_PTTL: &str = include_str!("../script/pttl.lua");
const LUA_REFRESH: &str = include_str!("../script/refresh.lua");
const LUA_RELEASE: &str = include_str!("../script/release.lua");

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

/// Lock represents an obtained, distributed lock.
pub struct Lock {
    rcli: MultiplexedConnection,

    pub keys: Vec<String>,
    pub value: String,
    pub token_len: usize,
    pub fence_token: i64,
}

impl Lock {
    /// obtain tries to obtain a new lock using a key with the given TTL.
    /// May return ErrorKind::NotObtained if not successful.
    pub async fn obtain(
        rcli: MultiplexedConnection,
        key: String,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock, Box<dyn Error>> {
        Lock::obtain_multi(rcli, Vec::from([key]), ttl, opt).await
    }

    /// obtain_multi tries to obtain a new lock using a key with the given TTL.
    /// If any of requested key are already locked, no additional keys are
    /// locked and ErrorKind::NotObtained is returned.
    /// May return ErrorKind::NotObtained if not successful.
    pub async fn obtain_multi(
        mut rcli: MultiplexedConnection,
        keys: Vec<String>,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<Lock, Box<dyn Error>> {
        let mut opt = opt.unwrap_or(Options {
            retry_strategy: RetryStrategy::NoRetry,
            meta_data: "".to_string(),
            token: "".to_string(),
            fence_key: "".to_string(),
        });
        if opt.token.is_empty() {
            opt.token = Lock::random_token();
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
        let sleep = time::sleep(ttl);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
             res = async {
                script
                 .key(&run_keys)
                 .arg((&value, token_len, ttl.as_millis(), fence))
                 .invoke_async(&mut rcli).await
             }
                 => {
                     match res
             {
                 Ok(res) => {
                     match res {
                        redis::Value::Int(fence_token) => {
                            return Ok(Lock {
                                rcli,
                                keys,
                                value,
                                token_len,
                                fence_token,
                            });
                        }
                        redis::Value::Okay => {
                            return Ok(Lock {
                                rcli,
                                keys,
                                value,
                                token_len,
                                fence_token: 0,
                            });
                        }
                        redis::Value::Nil => {
                            // obtain.lua return false, that means keys has set, and cannot be overwritten with a different token.
                            // so retry to get lock again, or no retry.
                            let backoff = opt.retry_strategy.next_back_off()?;
                             if backoff.is_zero() {
                                 return Err(ErrorKind::NotObtained.into());
                             }
                             tokio::time::sleep(backoff).await;
                        }
                        _=>{
                            unimplemented!()
                        }
                     }
                 }
                 Err(e) => {
                    return Err(e.into());
                    //  match e.kind() {
                    //      redis::ErrorKind::Server(_) => {
                    //          // any non-nil error from obtain is terminal (transient redis
                    //          // errors are unlikely to clear within a lock TTL and retrying a
                    //          // broken server is futile).
                    //          return Err(e.to_string().into());
                    //      }
                    //      _ => {
                    //          // retry
                    //          let backoff = opt.retry_strategy.next_back_off()?;
                    //          if backoff.is_zero() {
                    //              return Err(ErrorKind::NotObtained.into());
                    //          }
                    //          tokio::time::sleep(backoff).await;
                    //      }
                    //  }
                 }
             }
                 }
             () = &mut sleep => {
                return Err(ErrorKind::RetryTimeout.into())
             }
            }
        }
    }

    /// random_token generates a random token for the lock.
    fn random_token() -> String {
        let mut token = [0u8; 16];
        rand::fill(&mut token[..]);
        BASE64_URL_SAFE_NO_PAD.encode(token)
    }

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
    pub async fn ttl(&mut self) -> Result<Duration, Box<dyn Error>> {
        let script = redis::Script::new(LUA_PTTL);
        match script
            .key(&self.keys)
            .arg(&self.value)
            .invoke_async(&mut self.rcli)
            .await
        {
            Ok(res) => match res {
                redis::Value::Nil => {
                    unimplemented!()
                }
                redis::Value::Int(dur) => Ok(Duration::from_millis(dur as u64)),
                redis::Value::Okay => {
                    unimplemented!("redis::Value::Okay")
                }
                _ => {
                    unimplemented!()
                }
            },
            Err(e) => Err(e.into()),
        }
    }

    /// refresh extends the lock with a new ttl.
    /// May retrun ErrorKind::NotObtained if refresh is unsuccessful.
    pub async fn refresh(
        &mut self,
        ttl: Duration,
        opt: Option<Options>,
    ) -> Result<(), Box<dyn Error>> {
        let opt = opt.unwrap_or(Options {
            retry_strategy: RetryStrategy::NoRetry,
            meta_data: "".to_string(),
            token: "".to_string(),
            fence_key: "".to_string(),
        });
        let script = redis::Script::new(LUA_REFRESH);
        let sleep = time::sleep(ttl);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                res = async {
                    script
                .key(&self.keys)
                .arg(&self.value)
                .invoke_async::<u64>(&mut self.rcli).await
                } => {
                    match res
                    {
                        Ok(_) => return Ok(()),
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
                () = &mut sleep => {
                    return Err(ErrorKind::RetryTimeout.into());
                }
            }
        }
    }

    /// release manually releases the lock.
    /// May return ErrorKind::LockNotHeld if the lock is not held by the caller.
    pub async fn release(&mut self) -> Result<(), Box<dyn Error>> {
        let script = redis::Script::new(LUA_RELEASE);
        match script
            .key(&self.keys)
            .arg(&self.value)
            .invoke_async(&mut self.rcli)
            .await
        {
            Ok(res) => match res {
                redis::Value::Nil => {
                    unimplemented!()
                }
                redis::Value::Okay => Ok(()),
                _ => {
                    unimplemented!()
                }
            },
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod lock_tests {
    use super::*;
    use std::sync::OnceLock;

    /// TestContext 测试上下文
    struct TestContext {
        client: redis::Client,
        lock_inc: AtomicU64,           // lock increment
        keys: Arc<Mutex<Vec<String>>>, // redis keys
    }

    static TEST_CONTEXT: OnceLock<TestContext> = OnceLock::new();

    impl TestContext {
        /// new 新建测试上下文实例
        fn new() -> &'static Self {
            TEST_CONTEXT.get_or_init(|| Self {
                client: redis::Client::open("redis://127.0.0.1:6379").unwrap(),
                lock_inc: AtomicU64::new(0),
                keys: Arc::new(Mutex::new(vec![])),
            })
        }

        /// conn 获取redis连接实例
        async fn conn(&self) -> redis::aio::MultiplexedConnection {
            self.client
                .get_multiplexed_async_connection()
                .await
                .unwrap()
        }

        /// lock_key 生产一个redis lock key
        fn lock_key(&self) -> String {
            let s = format!(
                "_sfshf_rs_redislock_{}_",
                self.lock_inc.fetch_add(1, Ordering::Relaxed)
            );
            self.keys.lock().unwrap().push(s.clone());
            s
        }

        /// quick_obtain
        async fn quick_obtain(&self, ttl: Duration) -> Lock {
            Lock::obtain(self.conn().await, self.lock_key(), ttl, None)
                .await
                .unwrap()
        }

        /// del 删除redis keys
        async fn del(&self, keys: Vec<String>) {
            let mut conn = self.conn().await;
            redis::cmd("DEL")
                .arg(keys)
                .exec_async(&mut conn)
                .await
                .unwrap();
        }
    }

    /// assert_ttl 断言锁的生存时间（time to live）
    async fn assert_ttl<'a>(lock: &mut Lock, exp: Duration) {
        let ttl = lock.ttl().await.unwrap();
        assert!(ttl.abs_diff(exp).le(&Duration::from_secs(1)));
    }

    /// redis_conn 测试redis连接
    #[tokio::test(flavor = "current_thread")]
    async fn redis_command_and_script_calls() {
        let ctx = TestContext::new();
        let mut conn = ctx.conn().await;
        // redis commands
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
        ctx.del(vec!["foo".to_string()]).await;
        // redis script
        let script = redis::Script::new(
            r"
            return tonumber(ARGV[1]) + tonumber(ARGV[2]);
        ",
        );
        let result: Result<i32, _> = script.arg(&[1, 2]).invoke_async(&mut conn).await.unwrap();
        assert_eq!(result, Ok(3));
    }

    // client 测试客户端
    #[tokio::test(flavor = "current_thread")]
    async fn lock() {
        let ctx = TestContext::new();
        let conn = ctx.conn().await;
        let lk = ctx.lock_key();
        let mut lock = Lock::obtain(conn.clone(), lk.clone(), Duration::from_hours(1), None)
            .await
            .unwrap();
        // lock token
        assert_eq!(lock.token().len(), 22);
        // check TTL
        assert_ttl(&mut lock, Duration::from_hours(1)).await;
        // try to obtain again
        assert!(
            Lock::obtain(conn.clone(), lk.clone(), Duration::from_hours(1), None)
                .await
                .is_err()
        );
        // manually unlock
        assert!(lock.release().await.is_ok());
        // lock again
        let mut lock = Lock::obtain(conn.clone(), lk, Duration::from_hours(1), None)
            .await
            .unwrap();
        // unlock again
        assert!(lock.release().await.is_ok());
    }

    // obtain 测试获取锁
    #[tokio::test(flavor = "current_thread")]
    async fn obtain() {
        let ctx = TestContext::new();
        let mut lock = ctx.quick_obtain(Duration::from_hours(1)).await;
        assert!(lock.release().await.is_ok())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_metadata() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_custom_token() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_fence() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_fence_reentrant() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_success() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_failure() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_context_deadline() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_concurrent() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lock_refresh() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lock_release_expired() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lock_release_not_own() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lock_release_not_held() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lock_obtain_multi() {
        // let ctx = TestContext::new();
        // let mut conn = ctx.conn().await;
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
    RetryTimeout,
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
            RetryTimeout => "retry: timeout",
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
