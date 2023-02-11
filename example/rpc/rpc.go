package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/srshbhryn/gocelery"
)

func main() {
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://127.0.0.1:6379/3")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxIdle:         32,
		MaxActive:       32,
		Wait:            true,
		MaxConnLifetime: time.Duration(10 * time.Hour),
	}
	backend := gocelery.NewRedisRPCcBackend(redisPool)
	broker := gocelery.NewRedisBroker(redisPool)
	broker.QueueName = "trade_rpc"

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		broker,
		backend,
		16,
	)
	// prepare arguments
	taskName := "trade.get_balance"
	argA := "99994715bc10442f85a1de3a8bad9896"
	argB := "13"
	// run task
	var wg = sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			ar, _ := cli.Delay(taskName, argA, argB)
			x, _ := backend.GetResult(ar.TaskID)
			fmt.Println(x)
			wg.Done()

		}()
	}
	wg.Wait()
}
