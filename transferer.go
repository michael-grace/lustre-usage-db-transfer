package main

import (
	"time"
)

const NUM_WORKERS = 100;

type OldUsage struct {
	LustreVolume string
	PI string
	UnixGroup string
	Used int64
	Quota int64
	LastModified float32
	ArchivedDirectories string
	Date time.Date
	IsHumgen int
}

func main() {

	/** Plan
	Firstly, connect to the DB
	
	Secondly, extract all the PIs, unixgroups and scratch volumes,
	add them to new DB, get their new IDs and then store them here

	Thirdly, get the DB to start returning records for us to process

	Fourthly, spin up a load of workers to process each of these concurrently (to save time)
	as they come in, by adding the relevant data to the correct places in the new schema.
	*/

	// DB Connections

	// Other Table Data

	// Process all records
	jobs := make(chan OldUsage)
	var wg sync.WaitGroup

	wg.Add(NUM_WORKERS)
	for (i := 0; i<NUM_WORKERS; i++) {
		go transfer_worker(jobs, wg)
	}
	wg.Wait()
}