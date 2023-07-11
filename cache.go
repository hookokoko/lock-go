package lock_go

import (
	"context"
	"errors"
	"sync"
	"time"
)

/** TODO
1. LRU
2. LFU
3. 缓存模式验证
*/

type Cache[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, val T, expiration time.Duration) error
	Delete(ctx context.Context, key string) error
	Close(ctx context.Context) error
}

type LocalCache[T any] struct {
	data     map[string]*item[T]
	mux      sync.RWMutex
	interval time.Duration
	close    chan struct{}
}

type item[T any] struct {
	val      T
	deadline time.Time
}

type Option[T any] func(cache *LocalCache[T])

func WithExpiration[T any](duration time.Duration) Option[T] {
	return func(cache *LocalCache[T]) {
		cache.interval = duration
	}
}

func New[T any](opts ...Option[T]) *LocalCache[T] {
	lc := &LocalCache[T]{
		data:  make(map[string]*item[T]),
		mux:   sync.RWMutex{},
		close: make(chan struct{}),
	}
	for _, o := range opts {
		o(lc)
	}

	go func() {
		ticker := time.NewTimer(lc.interval)
		for {
		LOOP:
			select {
			case <-ticker.C:
				cnt := 0
				// 遍历所有的key，或者部分key
				lc.mux.Lock()
				for k, v := range lc.data {
					if cnt > 10000 {
						goto LOOP
					}
					if !v.deadline.IsZero() && v.deadline.Before(time.Now()) {
						delete(lc.data, k)
					}
					cnt++
				}
				lc.mux.Unlock()
			case <-lc.close:
				return
			}
		}
	}()

	return lc
}

func (l *LocalCache[T]) Get(_ context.Context, key string) (T, error) {
	var val T
	it, ok := l.data[key]
	if !ok {
		return val, errors.New("key not exist")
	}
	// 获取的时候检测，是否要删除
	if it.deadline.Before(time.Now()) {
		delete(l.data, key)
		return val, errors.New("key has expiated")
	}
	return it.val, nil
}

func (l *LocalCache[T]) Set(_ context.Context, key string, val T, expiration time.Duration) error {
	defer l.mux.Unlock()
	l.mux.Lock()

	it := &item[T]{val: val, deadline: time.Now().Add(expiration)}
	_, ok := l.data[key]
	if ok {
		return errors.New("exist key")
	}
	l.data[key] = it
	return nil
}

func (l *LocalCache[T]) Delete(_ context.Context, key string) error {
	defer l.mux.Unlock()
	l.mux.Lock()
	_, ok := l.data[key]
	if !ok {
		return errors.New("key not exist")
	}
	delete(l.data, key)
	return nil
}

func (l *LocalCache[T]) Close() error {
	close(l.close)
	return nil
}
