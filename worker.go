package main

import (
	"database/sql"
	"sync"
)

func transfer_worker(jobs <-chan OldUsage, wg sync.WaitGroup, keyData KeyData, db *sql.DB) {
	for job := range jobs {
		// Process the Transfer
		_, err := db.Exec("INSERT INTO hgi_lustre_usage.lustre_usage (used, quote, record_date, archived, last_modified, pi_id, unix_id, volume_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
			job.Used,
			job.Quota,
			job.Date,
			(job.ArchivedDirectories != ""),
			job.LastModified,
			keyData.PIs[job.PI],
			keyData.UnixGroups[job.UnixGroup],
			keyData.Volumes[job.LustreVolume])

		if err != nil {
			panic(err)
		}
	}
	wg.Done()
}
