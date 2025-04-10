package main

import (
	"context"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"slices"
	"sync"
	"time"
)

type Logs struct {
	db      map[string]*[][]float64
	commits *maelstrom.KV //commits map[string]float64 -- key1 -> int for offsets, $key1 -> int for commit
	*sync.Mutex
}

const PAGE_SIZE = 10

func (logs *Logs) Put(key string, value float64, nextOffset int) int {
	logs.Lock()
	defer logs.Unlock()

	//non-sync call, need the offset for this item
	if nextOffset < 0 {
		nextOffset = logs.GetSetNextOffset(key)
	}

	if _, ok := logs.db[key]; ok {
		newNode := []float64{float64(nextOffset), value}
		*logs.db[key] = append(*logs.db[key], newNode)
	} else {
		//create new arr
		logs.db[key] = &[][]float64{{float64(nextOffset), value}}
	}

	return nextOffset
}

func (logs *Logs) Poll(offsets map[string]float64) map[string][][]float64 {
	logs.Lock()
	defer logs.Unlock()

	response := map[string][][]float64{} //[] = [offset, value]
	for logKey, offsetValue := range offsets {
		dbLog, ok := logs.db[logKey]
		if !ok {
			continue
		}

		//get initial index of offset
		index, ok := slices.BinarySearchFunc(*dbLog, offsetValue, func(list []float64, target float64) int {
			return int(list[0] - target)
		})
		endIndex := min(len(*dbLog), index+PAGE_SIZE)

		keyResponse := make([][]float64, 0)
		for index < endIndex {
			entry := (*dbLog)[index]
			keyResponse = append(keyResponse, []float64{entry[0], entry[1]})

			index++
		}

		response[logKey] = keyResponse
	}

	return response
}

func (logs *Logs) Commit(offsets map[string]float64) {
	logs.Lock()
	defer logs.Unlock()

	for logKey, offsetValue := range offsets {
		logs.SetNextKVValue("$"+logKey, offsetValue)
	}
}

func (logs *Logs) ListCommits(keys []string) map[string]float64 {
	logs.Lock()
	defer logs.Unlock()

	response := make(map[string]float64)
	for _, key := range keys {
		commitKey := "$" + key

		responseValue, err := logs.commits.ReadInt(context.Background(), commitKey)
		//check for valid Read as we can get reqs for non-committed keys
		if err == nil {
			response[key] = float64(responseValue)
		}
	}

	return response
}

func (logs *Logs) GetSetNextOffset(key string) int {
	//this should be called from inside a function with a lock, no retries because we assume a good network
	maxRetries := 20
	for range maxRetries {
		thisCommit, _ := logs.commits.ReadInt(context.Background(), key)
		err := logs.commits.CompareAndSwap(context.Background(), key, thisCommit, thisCommit+1, true)

		if err == nil {
			return thisCommit + 1
		}
		time.Sleep(100 * time.Millisecond)
	}

	appendToLogFile(ErrorMessage{"Ran out of retries on  commit update: " + key})
	return -1
}

func (logs *Logs) SetNextKVValue(key string, next float64) {
	maxRetries := 20
	for range maxRetries {
		thisCommit, _ := logs.commits.ReadInt(context.Background(), key)

		//another node might set a higher value, we want to make sure twe don't overwrite again
		if thisCommit < int(next) {
			err := logs.commits.CompareAndSwap(context.Background(), key, thisCommit, next, true)

			if err == nil {
				return //success
			}
		} else {
			return //newer, higher update happened on commit key
		}
		time.Sleep(100 * time.Millisecond)
	}

	appendToLogFile(ErrorMessage{"Ran out of retries on KVValue update: " + key})
}

type ErrorMessage struct {
	Message string `json:"message"`
}
