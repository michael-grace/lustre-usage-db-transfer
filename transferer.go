package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	NUM_WORKERS = 100
)

type OldUsage struct {
	LustreVolume        string
	PI                  string
	UnixGroup           string
	Used                int64
	Quota               int64
	LastModified        float32
	ArchivedDirectories string
	Date                time.Time
	IsHumgen            int
}

type KeyData struct {
	PIs, Volumes, UnixGroups map[string]int
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

	db_creds := struct {
		Host, User, Pass, Name string
		Port                   int
	}{
		Host: "",
		Port: 1234,
		User: "",
		Pass: "",
		Name: "",
	}

	// DB Connections
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf(
			"%s:%s@%s:%v/%s",
			db_creds.User,
			db_creds.Pass,
			db_creds.Host,
			db_creds.Port,
			db_creds.Name))

	if err != nil {
		panic(err)
	}

	defer db.Close()

	// Other Table Data
	var results *sql.Rows

	// PIs
	results, err = db.Query("SELECT DISTINCT PI from lustre_usage")
	if err != nil {
		panic(err)
	}

	pis := make(map[string]int)

	for results.Next() {
		var pi string
		err = results.Scan(&pi)

		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO hgi_lustre_usage.pi (pi_name) VALUES (?);", pi)

		if err != nil {
			panic(err)
		}

		var new_pi_id int

		err = db.QueryRow("SELECT pi_id FROM hgi_lustre_usage.pi WHERE pi_name = ?", pi).Scan(&new_pi_id)
		if err != nil {
			panic(err)
		}

		pis[pi] = new_pi_id

	}

	// Unixgroups
	results, err = db.Query("SELECT DISTINCT `Unix Group` from lustre_usage")
	if err != nil {
		panic(err)
	}

	unixgroups := make(map[string]int)

	for results.Next() {
		var unixgroup string
		err = results.Scan(&unixgroup)

		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO hgi_lustre_usage.unix_group (group_name) VALUES (?);", unixgroup)

		if err != nil {
			panic(err)
		}

		var new_group_id int

		err = db.QueryRow("SELECT group_id FROM hgi_lustre_usage.unix_group WHERE group_name = ?", unixgroup).Scan(&new_group_id)
		if err != nil {
			panic(err)
		}

		unixgroups[unixgroup] = new_group_id
	}

	// Lustre Volumes
	results, err = db.Query("SELECT DISTINCT `Lustre Volume` from lustre_usage")
	if err != nil {
		panic(err)
	}

	volumes := make(map[string]int)

	for results.Next() {
		var volume string
		err = results.Scan(&volume)

		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO hgi_lustre_usage.volume (volume_name) VALUES (?);", volume)

		if err != nil {
			panic(err)
		}

		var new_vol_id int

		err = db.QueryRow("SELECT volume_id FROM hgi_lustre_usage.volume WHERE volume_name = ?", volume).Scan(&new_vol_id)
		if err != nil {
			panic(err)
		}

		volumes[volume] = new_vol_id
	}

	fmt.Println(pis, unixgroups, volumes)
	keyData := KeyData{
		PIs:        pis,
		Volumes:    volumes,
		UnixGroups: unixgroups,
	}

	// Process all records
	jobs := make(chan OldUsage, NUM_WORKERS)
	var wg sync.WaitGroup

	wg.Add(NUM_WORKERS)
	for i := 0; i < NUM_WORKERS; i++ {
		go transfer_worker(jobs, wg, keyData, db)
	}

	// Check the query
	bigQuery, err := db.Query("SELECT (`Lustre Volume`, PI, `Unix Group`, Used, Quota, `Last Modified`, `Archived Directories`, Date, `Is Humgen`) FROM lustre_usage WHERE date > 2021-01-01")

	if err != nil {
		panic(err)
	}

	for bigQuery.Next() {
		var dataPoint OldUsage
		err = bigQuery.Scan(
			&dataPoint.LustreVolume,
			&dataPoint.PI,
			&dataPoint.UnixGroup,
			&dataPoint.Used,
			&dataPoint.Quota,
			&dataPoint.LastModified,
			&dataPoint.ArchivedDirectories,
			&dataPoint.Date,
			&dataPoint.IsHumgen,
		)

		if err != nil {
			panic(err)
		}

		jobs <- dataPoint
	}
	close(jobs)

	wg.Wait()
}
