package lock_go

import (
	"context"
	"github.com/ecodeclub/ekit/retry"
	"github.com/golang/mock/gomock"
	"github.com/lock-go/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClient_TryAcquireLock(t *testing.T) {
	testCases := []struct {
		name       string
		client     Client
		key        string
		val        any
		expiration time.Duration
		before     func()
	}{
		{
			name:       "try lock",
			client:     Client{client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})},
			key:        "keys1",
			expiration: time.Minute,
			before:     func() {},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			_, err := tc.client.TryAcquireLock(context.Background(), tc.key, tc.expiration)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLock_Unlock(t *testing.T) {
	testCases := []struct {
		name       string
		client     *Client
		key        string
		val        any
		expiration time.Duration
		before     func()
	}{
		{
			name:       "try lock",
			client:     &Client{client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})},
			key:        "keys1",
			expiration: time.Minute,
			before:     func() {},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			l, err := tc.client.TryAcquireLock(context.Background(), tc.key, tc.expiration)
			if err != nil {
				t.Fatal(err)
			}
			err = l.Unlock(context.Background())
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLock_Refresh(t *testing.T) {
	testCases := []struct {
		name       string
		client     *Client
		key        string
		val        any
		expiration time.Duration
		before     func()
	}{
		{
			name:       "try lock",
			client:     &Client{client: redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})},
			key:        "keys1",
			expiration: time.Minute,
			before:     func() {},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			l, err := tc.client.TryAcquireLock(context.Background(), tc.key, tc.expiration)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 10; i++ {
				err = l.Refresh(context.Background())
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestLock_AutoRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testCases := []struct {
		name     string
		lock     func() *Lock
		interval time.Duration
		timeout  time.Duration
		wantErr  error
	}{
		{
			name: "超时重试且超过最大次数",
			lock: func() *Lock {
				mockRdb := mocks.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background()) // 构建mock的返回值
				cmd.SetErr(context.DeadlineExceeded)
				mockRdb.EXPECT().Eval(gomock.Any(), luaRefresh, gomock.Any(), gomock.Any()).Return(cmd).AnyTimes()
				return &Lock{
					Client:     &Client{client: mockRdb},
					expiration: time.Minute,
					unlockCh:   make(chan struct{}),
				}
			},
			interval: 3 * time.Second,
			timeout:  time.Second,
			wantErr:  ErrRefreshTimeout,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.lock()
			err := l.AutoRefresh(context.Background(), tc.interval, tc.timeout)
			assert.Equal(t, tc.wantErr, err)
		})
	}

}

func TestClient_AcquireLock(t *testing.T) {
	client := NewClient()
	fixRetry, err := retry.NewFixedIntervalRetryStrategy(2*time.Second, 3000)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AcquireLock(context.Background(), "keys_1", time.Minute, fixRetry)
	if err != nil {
		t.Fatal(err)
	}

	//err = l.Unlock(context.Background())
	//
	//if err != nil {
	//	t.Fatal(err)
	//}
}
