package main

import (
	"fmt"
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
	couter := 0
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		go func() {
			ar, err := cli.Delay(taskName, argA, argB)
			if err != nil {
				panic(err)
			}
			r, err := backend.GetResult(ar.TaskID)
			fmt.Println(r)
			couter++
			fmt.Println(couter)
		}()
	}
	time.Sleep(30 * time.Second)
}
