package main

import (
	"database/sql"
	"sync"
)

func transfer_worker(jobs <-chan OldUsage, wg *sync.WaitGroup, keyData KeyData, db *sql.DB) {
	for job := range jobs {
		// Process the Transfer

		var pi interface{}
		// Deal with NULL PI
		if job.PI.Valid {
			pi = keyData.PIs[job.PI.String]
		} else {
			pi = nil
		}

		_, err := db.Exec("INSERT INTO hgi_lustre_usage_new.lustre_usage (used, quota, record_date, archived, last_modified, pi_id, unix_id, volume_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
			job.Used,
			job.Quota,
			job.Date,
			job.ArchivedDirectories.Valid,
			job.LastModified,
			pi,
			keyData.UnixGroups[job.UnixGroup][job.IsHumgen == 1],
			keyData.Volumes[job.LustreVolume])

		if err != nil {
			panic(err)
		}
	}
	wg.Done()
}
