package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/srshbhryn/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
func main() {

	// create redis connection pool
	redisPool := &redis.Pool{
		MaxIdle:     3,                 // maximum number of idle connections in the pool
		MaxActive:   0,                 // maximum number of connections allocated by the pool at a given time
		IdleTimeout: 240 * time.Second, // close connections after remaining idle for this duration
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL("redis://")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	backend := gocelery.NewRedisRPCcBackend(redisPool)
	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(redisPool),
		backend,
		1,
	)
	// prepare arguments
	taskName := "hw.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	ar, err := cli.Delay(taskName, argA, argB)
	if err != nil {
		panic(err)
	}
	r, err := backend.GetResult(ar.TaskID)
	fmt.Println(r)

}
