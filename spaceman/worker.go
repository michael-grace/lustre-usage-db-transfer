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

		var new_record_id int
		// Technically, this might not be unique, but searching by directory is a big no-no cause it could be NULL
		// So, searching by number of files because it is _very probably_ unique
		err = db.QueryRow("SELECT directory_id FROM hgi_lustre_usage_new.directory WHERE project_name = ? AND num_files = ?", job.Project, job.Files).Scan(&new_record_id)
		if err != nil {
			panic(err)
		}

		// File Types
		// Just hardcoding these foreign keys, not pulling from the DB

		/**

		BAM: 	1
		CRAM: 	2
		VCF: 	3
		PEDBED: 4

		*/

		_, err = db.Exec(`INSERT INTO hgi_lustre_usage_new.file_size (directory_id, filetype_id, size)
		VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)`,
			new_record_id,
			1,
			job.BAM,
			new_record_id,
			2,
			job.CRAM,
			new_record_id,
			3,
			job.VCF,
			new_record_id,
			4,
			job.PEDBED,
		)

		if err != nil {
			panic(err)
		}
	}

	wg.Done()
}
