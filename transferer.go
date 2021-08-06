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

	var pis []string
	for results.Next() {
		var pi string
		err = results.Scan(&pi)
		if err != nil {
			panic(err)
		}
		pis = append(pis, pi)
	}

	// Unixgroups
	results, err = db.Query("SELECT DISTINCT `Unix Group` from lustre_usage")
	if err != nil {
		panic(err)
	}

	var unixgroups []string
	for results.Next() {
		var unixgroup string
		err = results.Scan(&unixgroup)
		if err != nil {
			panic(err)
		}
		unixgroups = append(unixgroups, unixgroup)
	}

	// Lustre Volumes
	results, err = db.Query("SELECT DISTINCT `Lustre Volume` from lustre_usage")
	if err != nil {
		panic(err)
	}

	var volumes []string
	for results.Next() {
		var volume string
		err = results.Scan(&volume)
		if err != nil {
			panic(err)
		}
		volumes = append(volumes, volume)
	}

	fmt.Println(pis, unixgroups, volumes)

	// Process all records
	jobs := make(chan OldUsage)
	var wg sync.WaitGroup

	wg.Add(NUM_WORKERS)
	for i := 0; i < NUM_WORKERS; i++ {
		go transfer_worker(jobs, wg)
	}
	wg.Wait()
}
