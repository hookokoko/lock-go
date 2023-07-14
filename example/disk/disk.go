package main

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/retry"
	"github.com/lock-go"
	"log"
	"time"
)

type MachineInfo struct {
	IP         string
	HostName   string
	DiskNum    []Disk
	Status     string // repairing|repaired|wait
	RepairTime time.Time
}

type Disk struct {
	Device string
	Num    int
	Health string // Bad | Good
}

func repair(ctx context.Context) {
	repairTotal := make([]MachineInfo, 0, 32)
	repairTotal = append(repairTotal, MachineInfo{})
	c := lock_go.NewClient()
	for _, info := range repairTotal {
		mi := info
		go func() {
			rey, _ := retry.NewFixedIntervalRetryStrategy(time.Hour, 1000)
			// 阻塞抢锁
			l, err := c.AcquireLock(ctx, mi.IP, time.Hour, rey)
			// - 抢到了，发起维修
			if err == nil {
				repairSingle(mi) // 这里也是一个阻塞着的任务，维修完成，会给一个请求过来，执行done()函数解锁
				//_ = l.Refresh(ctx)
				_ = l.AutoRefresh(context.Background(), 10*time.Minute, 5*time.Second) // 时间太久的话就手动del。。。
				//_ = l.Unlock(ctx) // 这里不用这会儿加，等维修完执行解锁。但是在其他进程中解锁时，l的key，咱是不知道的。。。,但是可以算出来key
			}
		}()
	}
}

func done(ctx context.Context, mi MachineInfo) error {
	key := getKeyBy(mi)
	val := getValBy(mi)
	l := lock_go.NewLock(key, val)
	return l.Unlock(ctx)
}

func getKeyBy(mi MachineInfo) string { return "" }
func getValBy(mi MachineInfo) any    { return nil }
func repairSingle(mi MachineInfo)    {}

func main() {
	c := lock_go.NewClient()
	ctx := context.Background()

	idc := "sz"
	ip := "172.10.24.111" // 需要换成网段

	// machine number 是待维修机器的唯一编号，
	// 它的获取，是定时从机器上拉取，放到db进行集中式管理，但是这里的更新时间是一个问题。
	machineNum := 12394
	N := 18
	K := 9
	// 通过取余的方式，获取多个key，意味着我能同时n-k个(锁)任务
	// 这种的话，如果machineNum分布不均，数据倾斜也是一个问题
	idx := machineNum % (N - K)

	key := fmt.Sprintf("%s-%s-%d", idc, ip, idx)
	expiration := time.Hour
	l, err := c.TryAcquireLock(ctx, key, expiration)
	if err != nil {
		log.Fatal(err)
	}
	// 维修发起

	// 维修完成
	err = l.Unlock(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
