package gocelery

import "sync"

var rwMutexePool = sync.Pool{
	New: func() interface{} {
		return &sync.RWMutex{}
	},
}
