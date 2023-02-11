package gocelery

import (
	"encoding/base64"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type RedisRPRCBackend struct {
	*redis.Pool
	resultsChannels map[string]chan *ResultMessage
	mutex           *sync.Mutex
}

func NewRedisRPCcBackend(conn *redis.Pool) *RedisRPRCBackend {
	backend := &RedisRPRCBackend{
		Pool:            conn,
		resultsChannels: make(map[string]chan *ResultMessage, 1024),
		mutex:           &sync.Mutex{},
	}
	go backend.fetchResultsWorker()
	return backend
}

func (cb *RedisRPRCBackend) GetResult(taskID string) (*ResultMessage, error) {

	cb.mutex.Lock()
	_, ok := cb.resultsChannels[taskID]
	if !ok {
		cb.resultsChannels[taskID] = make(chan *ResultMessage, 1)
	}
	cb.mutex.Unlock()

	val := <-cb.resultsChannels[taskID]
	close(cb.resultsChannels[taskID])
	delete(cb.resultsChannels, taskID)
	return val, nil
}

func (cb *RedisRPRCBackend) GetResultWithTimeOut(taskID string, timeout time.Duration) *ResultMessage {
	cb.mutex.Lock()
	_, ok := cb.resultsChannels[taskID]
	if !ok {
		cb.resultsChannels[taskID] = make(chan *ResultMessage, 1)
	}
	cb.mutex.Unlock()
	select {
	case val := <-cb.resultsChannels[taskID]:
		close(cb.resultsChannels[taskID])
		delete(cb.resultsChannels, taskID)
		return val
	case <-time.After(timeout):
		return nil
	}
}

func (cb *RedisRPRCBackend) fetchResultsWorker() {
	conn := cb.Get()
	defer conn.Close()
	for {
		val, err := conn.Do("BRPOP", myUUid, "0.0")
		if err != nil {
			log.Printf("RedisRPRCBackend fetchResultsWorker connection error %s\n", err)
			go cb.fetchResultsWorker()
			return
		}
		res, ok := val.([]interface{})
		if !ok {
			log.Printf("RedisRPRCBackend fetchResultsWorker invalid brpop response %s\n", res)
			go cb.fetchResultsWorker()
			return
		}
		response := make(map[string]interface{})
		err = json.Unmarshal(res[1].([]byte), &response)
		if err != nil {
			log.Printf("RedisRPRCBackend fetchResultsWorker invalid brpop response %s\n", res)
			go cb.fetchResultsWorker()
			return
		}
		decodedBody, err := base64.StdEncoding.DecodeString(response["body"].(string))
		if err != nil {
			log.Printf("RedisRPRCBackend fetchResultsWorker invalid brpop response %s\n", res)
			go cb.fetchResultsWorker()
			return
		}

		resultMessage := resultMessagePool.Get().(*ResultMessage)
		err = json.Unmarshal(decodedBody, &resultMessage)
		if err != nil {
			log.Println("RedisRPRCBackend fetchResultsWorker cant unmarshal")
			go cb.fetchResultsWorker()
			return
		}
		cb.mutex.Lock()
		_, ok = cb.resultsChannels[resultMessage.ID]
		if !ok {
			cb.resultsChannels[resultMessage.ID] = make(chan *ResultMessage, 1)
		}
		cb.mutex.Unlock()
		cb.resultsChannels[resultMessage.ID] <- resultMessage
	}
}

func (cb *RedisRPRCBackend) SetResult(taskID string, result *ResultMessage) error {
	panic("not implemented")
}
