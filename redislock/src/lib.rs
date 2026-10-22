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
use tokio::time;

/// lua scripts
const LUA_OBTAIN: &str = include_str!("obtain.lua");
const LUA_PTTL: &str = include_str!("pttl.lua");
const LUA_REFRESH: &str = include_str!("refresh.lua");
const LUA_RELEASE: &str = include_str!("release.lua");

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
                redis::Value::Nil => Err(ErrorKind::LockNotHeld.into()),
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
    use tokio::sync::Barrier;

    use crate::{
        LimitedRetry,
        RetryStrategy::{LinearBackoff, NoRetry},
    };

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

        /// cleanup 清理redis keys
        async fn cleanup(&self, keys: Vec<String>) {
            let mut conn = self.conn().await;
            _ = redis::cmd("DEL").arg(keys).exec_async(&mut conn).await;
        }
    }

    /// assert_ttl 断言锁的生存时间（time to live）
    async fn assert_ttl<'a>(lock: &mut Lock, exp: Duration) {
        let ttl = lock.ttl().await.unwrap();
        assert!(ttl.abs_diff(exp).le(&Duration::from_secs(1)));
    }

    // lock 测试Lock
    #[tokio::test(flavor = "current_thread")]
    async fn lock() {
        let ctx = TestContext::new();
        let conn = ctx.conn().await;
        let key = ctx.lock_key();
        let mut lock = Lock::obtain(conn.clone(), key.clone(), Duration::from_hours(1), None)
            .await
            .unwrap();
        // lock token
        assert_eq!(lock.token().len(), 22);
        // check TTL
        assert_ttl(&mut lock, Duration::from_hours(1)).await;
        // try to obtain again
        assert!(
            Lock::obtain(conn.clone(), key.clone(), Duration::from_hours(1), None)
                .await
                .is_err()
        );
        // manually unlock
        assert!(lock.release().await.is_ok());
        // lock again
        let mut lock = Lock::obtain(conn.clone(), key.clone(), Duration::from_hours(1), None)
            .await
            .unwrap();
        // unlock again
        assert!(lock.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(vec![key]).await;
    }

    // obtain 测试获取锁
    #[tokio::test(flavor = "current_thread")]
    async fn obtain() {
        let ctx = TestContext::new();
        let mut lock = ctx.quick_obtain(Duration::from_hours(1)).await;
        assert!(lock.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(lock.keys()).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_metadata() {
        let ctx = TestContext::new();
        let meta = String::from("meta_data");
        let mut lock = Lock::obtain(
            ctx.conn().await,
            ctx.lock_key(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: meta.clone(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(lock.meta_data(), meta);
        assert!(lock.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(lock.keys()).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_custom_token() {
        let ctx = TestContext::new();
        let key = ctx.lock_key();
        let meta = String::from("obtain_custom_token_meta_data");
        let token = String::from("obtain_custom_token");
        let mut lock = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: meta.clone(),
                token: token.clone(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(lock.token(), token);
        assert_eq!(lock.meta_data(), meta);

        // try to obtain again
        let Err(err) =
            Lock::obtain(ctx.conn().await, key.clone(), Duration::from_hours(1), None).await
        else {
            panic!("not error");
        };
        if let Some(e) = err.downcast_ref::<ErrorKind>() {
            assert_eq!(e, &ErrorKind::NotObtained);
        } else {
            panic!("not custom error");
        }

        // allow to re-obtain lock if token is known
        let meta2 = String::from("obtain_custom_token_meta_data_2");
        let mut lock2 = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: meta2.clone(),
                token: token.clone(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(lock2.token(), token);
        assert_eq!(lock2.meta_data(), meta2);

        // release locks
        let Err(err) = lock.release().await else {
            panic!("not error");
        };
        if let Some(e) = err.downcast_ref::<ErrorKind>() {
            assert_eq!(e, &ErrorKind::LockNotHeld);
        } else {
            panic!("not custom error");
        }
        assert!(lock2.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(vec![key]).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_fence() {
        let ctx = TestContext::new();

        // no token without a fence key
        let mut lock = ctx.quick_obtain(Duration::from_hours(1)).await;
        assert_eq!(lock.fence_token(), 0);
        assert!(lock.release().await.is_ok());

        let key: String = ctx.lock_key();
        let fence_key = key.clone() + ":fence";

        // first fenced obtain
        let mut lock = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: String::new(),
                fence_key: fence_key.clone(),
            }),
        )
        .await
        .unwrap();
        let tok1 = lock.fence_token();
        assert_eq!(tok1, 1);
        assert!(lock.release().await.is_ok());

        // next obtain must mint a greater token
        let mut lock = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: String::new(),
                fence_key: fence_key.clone(),
            }),
        )
        .await
        .unwrap();
        let tok2 = lock.fence_token();
        if tok2 <= tok1 {
            panic!("expected fence token > {}, got {}", tok1, tok2);
        }
        assert!(lock.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(vec![key, fence_key]).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_fence_reentrant() {
        let ctx = TestContext::new();
        let key: String = ctx.lock_key();
        let fence_key = key.clone() + ":fence";
        let token = String::from("obtain_fence_reentrant");

        // obtain with a known token
        let lock = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: token.clone(),
                fence_key: fence_key.clone(),
            }),
        )
        .await
        .unwrap();
        let tok1 = lock.fence_token();
        assert_eq!(tok1, 1);
        // assert!(lock.release().await.is_ok()); // NOTE: don't release the previous lock

        // re-obtain (override) must not advance the token
        let mut lock = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: token.clone(),
                fence_key: fence_key.clone(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(lock.fence_token(), tok1);
        assert!(lock.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(vec![key, fence_key]).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_success() {
        let ctx = TestContext::new();
        let key: String = ctx.lock_key();

        // obtain for 20ms
        _ = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_millis(20),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();

        // lock again with linar retry -- 3x for 20ms
        let mut lock2 = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: RetryStrategy::LimitedRetry(Box::new(LimitedRetry::new(
                    LinearBackoff(Duration::from_millis(20)),
                    3,
                ))),
                meta_data: String::new(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();
        assert!(lock2.release().await.is_ok());

        // cleanup test context
        ctx.cleanup(vec![key]).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_failure() {
        let ctx = TestContext::new();
        let key: String = ctx.lock_key();

        // obtain for 20ms
        _ = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_millis(50),
            Some(Options {
                retry_strategy: NoRetry,
                meta_data: String::new(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        .unwrap();

        // lock again with linar retry -- 3x for 20ms
        let Err(err) = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_hours(1),
            Some(Options {
                retry_strategy: RetryStrategy::LimitedRetry(Box::new(LimitedRetry::new(
                    LinearBackoff(Duration::from_millis(5)),
                    3,
                ))),
                meta_data: String::new(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        else {
            panic!("not error");
        };
        if let Some(e) = err.downcast_ref::<ErrorKind>() {
            assert_eq!(e, &ErrorKind::NotObtained);
        } else {
            panic!("not custom error");
        }

        // cleanup test context
        ctx.cleanup(vec![key]).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn obtain_retry_context_deadline() {
        let ctx = TestContext::new();
        let key: String = ctx.lock_key();

        // obtain for 20ms
        _ = Lock::obtain(ctx.conn().await, key.clone(), Duration::from_hours(1), None)
            .await
            .unwrap();

        // lock again with linar retry -- 3x for 20ms
        let Err(err) = Lock::obtain(
            ctx.conn().await,
            key.clone(),
            Duration::from_millis(20),
            Some(Options {
                retry_strategy: RetryStrategy::LinearBackoff(Duration::from_millis(5)),
                meta_data: String::new(),
                token: String::new(),
                fence_key: String::new(),
            }),
        )
        .await
        else {
            panic!("not error");
        };
        if let Some(e) = err.downcast_ref::<ErrorKind>() {
            assert_eq!(e, &ErrorKind::RetryTimeout);
        } else {
            panic!("not custom error");
        }

        // cleanup test context
        ctx.cleanup(vec![key]).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn obtain_concurrent() {
        let ctx = TestContext::new();
        let key: String = ctx.lock_key();
        let tasks = 20;
        let barrier = Arc::new(Barrier::new(tasks));
        let mut handles = Vec::with_capacity(tasks);
        for i in 0..tasks {
            let c = barrier.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                c.wait().await;
                let get =
                    match Lock::obtain(ctx.conn().await, key, Duration::from_mins(1), None).await {
                        Ok(_) => 1,
                        Err(err) => {
                            if let Some(e) = err.downcast_ref::<ErrorKind>() {
                                match e {
                                    &ErrorKind::NotObtained => 0,
                                    _ => {
                                        panic!("{}", e);
                                    }
                                }
                            } else {
                                panic!("not custom error");
                            }
                        }
                    };
                c.wait().await;
                get
            }));
        }
        let mut num_locks = 0;
        for handle in handles {
            num_locks += handle.await.unwrap();
        }
        assert_eq!(num_locks, 1);

        // cleanup test context
        ctx.cleanup(vec![key]).await;
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
