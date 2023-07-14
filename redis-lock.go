package lock_go

import (
	"context"
	_ "embed"
	"errors"
	"github.com/ecodeclub/ekit/retry"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	//go:embed lua/lock.lua
	luaLock string

	//go:embed lua/unlock.lua
	luaUnlock string

	//go:embed lua/refresh.lua
	luaRefresh string
)

var (
	ErrHoldLock       = errors.New("redis-lock: 锁已被持有")
	ErrNoHoldLock     = errors.New("redis-lock: 锁未被持有")
	ErrRefreshTimeout = errors.New("redis-lock: 续约超时超过重试次数")
	ErrWaitLockFail   = errors.New("redis-lock: 等待占有锁失败")
	ErrLockTimeout    = errors.New("redis-lock: 等待占有锁超时")
)

type Retryable retry.Strategy

type Client struct {
	client redis.Cmdable
}

type Lock struct {
	*Client
	key        string
	val        any
	expiration time.Duration
	unlockCh   chan struct{}
}

func NewLock(key string, val any) *Lock {
	return &Lock{
		key: key,
		val: val,
	}
}

func NewClient() *Client {
	return &Client{client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})}
}

// TryAcquireLock 非阻塞加锁,加锁失败就退出
func (c *Client) TryAcquireLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	val := uuid.New().String()
	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		// 锁已被别人持有
		return nil, ErrHoldLock
	}

	l := &Lock{
		Client:     c,
		key:        key,
		val:        val,
		expiration: expiration,
		unlockCh:   make(chan struct{}),
	}

	return l, nil
}

// AcquireLock 阻塞式加锁，直到获取到锁 或者 异常退出
func (c *Client) AcquireLock(ctx context.Context, key string, expiration time.Duration, retry Retryable) (*Lock, error) {
	val := uuid.New().String()
	var timer *time.Ticker
	for {
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		res, err := c.client.Eval(timeoutCtx, luaLock, []string{key}, val, expiration.Seconds()).Result()
		cancel()
		// 加锁成功了
		if res == "OK" {
			return &Lock{
				Client:     c,
				key:        key,
				val:        val,
				expiration: expiration,
				unlockCh:   make(chan struct{}),
			}, nil
		}
		td, ok := retry.Next()
		// 达到最大重试次数
		if !ok {
			// 1.锁已经被占有，再次重试，此时err==nil，但是res != OK
			if err == nil {
				return nil, ErrWaitLockFail
			}
			// 2.最后一次执行lua超时了，再次重试
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, ErrLockTimeout
			}
			// 3. 其他error
			return nil, err
		}

		if timer == nil {
			timer = time.NewTicker(td)
		} else {
			timer.Reset(td)
		}
		select {
		case <-timer.C:
		// 可能被取消了，不再执行retry
		// 如果是超时，继续retry, 直到达到最大限制
		// 注意不是timeoutCtx
		case <-ctx.Done():
			if errors.Is(timeoutCtx.Err(), context.Canceled) {
				return nil, timeoutCtx.Err()
			}
		}
	}
}

func (l *Lock) Unlock(ctx context.Context) error {
	defer func() {
		select {
		// 告诉AutoRefresh 该停停了
		case l.unlockCh <- struct{}{}:
		default:
		}
	}()
	// get & del
	res, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.val).Int64()
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrNoHoldLock
	}
	return nil
}

func (l *Lock) Refresh(ctx context.Context) error {
	// get & set
	res, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.val, l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrNoHoldLock
	}
	return nil
}

func (l *Lock) AutoRefresh(ctx context.Context, interval time.Duration, timeout time.Duration) error {
	maxRetryCnt := 3
	timeoutCh := make(chan struct{}, maxRetryCnt)
	// 要保证l.expiration < interval + timeout，不然续约的时候，我的key可能到期就没了
	ticker := time.NewTicker(interval)
	timeoutCnt := 0
	for {
		select {
		case <-ticker.C:
			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			err := l.Refresh(timeoutCtx)
			cancel()
			// 如果是续约超时了, 是重试还是退出，重试的话要重试多少次？如果重试还超时呢？
			if errors.Is(err, context.DeadlineExceeded) {
				timeoutCh <- struct{}{}
				continue
			}
			// 续约出现错误时，要通知到业务
			if err != nil {
				return err
			}
		// 这里timeoutCh容量有讲究
		// 因为超时会重试，所以重试次数就是timeoutCh的容量
		// 因为当执行了执行maxRetryCnt次时，写入timeoutCh的数据最多可能有maxRetryCnt个，说明maxRetryCnt个ch没有来得及处理
		case <-timeoutCh:
			timeoutCnt++
			//fmt.Println("retry:", timeoutCnt)
			if timeoutCnt >= maxRetryCnt {
				return ErrRefreshTimeout
			}
			timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
			err := l.Refresh(timeoutCtx)
			cancel()
			// 如果是续约还是超时了
			if errors.Is(err, context.DeadlineExceeded) {
				timeoutCh <- struct{}{}
				continue
			}
			// 续约出现错误时，要通知到业务
			if err != nil {
				return err
			}
		case <-l.unlockCh:
			return nil
		}
	}
}
