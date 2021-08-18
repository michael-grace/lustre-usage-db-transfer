package main

import (
	"database/sql"
	"fmt"
	"sync"
)

func transfer_worker(jobs <-chan OldRecord, wg *sync.WaitGroup, keyData KeyData, db *sql.DB) {
	for job := range jobs {

		// Deal with Directory
		// NULL instead of "*TOTAL*"
		var directory interface{}
		if job.Directory == "*TOTAL*" {
			directory = nil
		} else {
			directory = job.Directory
		}

		// Deal with NULL PI
		var pi interface{}
		if job.PI == "-" {
			pi = nil
		} else {
			pi = keyData.PIs[job.PI]
		}

		// Deal with NULL Unixgroup
		var group interface{}
		if job.UnixGroup == "-" {
			group = nil
		} else {
			group = keyData.UnixGroups[job.UnixGroup][1]
		}

		// Add the directory
		_, err := db.Exec("INSERT INTO hgi_lustre_usage_new.directory (project_name, directory_path, num_files, size, last_modified, pi_id, volume_id, group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
			job.Project,
			directory,
			job.Files,
			job.Total,
			job.LastModified,
			pi,
			keyData.Volumes[fmt.Sprintf("scratch%v", job.Volume)],
			group,
		)

		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}
