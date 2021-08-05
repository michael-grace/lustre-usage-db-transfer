package main

import (
	"sync"
)

func transfer_worker(jobs <-chan OldUsage, wg sync.WaitGroup) {
	for job := range jobs {
		// Process the Transfer
	}
	wg.Done()
}