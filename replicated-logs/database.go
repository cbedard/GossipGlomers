package main

import (
	"slices"
	"sync"
)

var PAGE_SIZE = 10

type Logs struct {
	db      map[string]*[][]float64
	commits map[string]float64
	*sync.Mutex
}

func (logs *Logs) Put(key string, value float64) int {
	logs.Lock()
	defer logs.Unlock()

	nextOffset := 0

	if _, ok := logs.db[key]; ok {
		nextOffset = len(*logs.db[key])

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
		logs.commits[logKey] = offsetValue
	}
}

func (logs *Logs) ListCommits(keys []string) map[string]float64 {
	logs.Lock()
	defer logs.Unlock()

	response := make(map[string]float64)

	for _, key := range keys {
		response[key] = logs.commits[key]
	}
	return response
}
