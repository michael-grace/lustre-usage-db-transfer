package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
)

const (
	NUM_WORKERS = 100
)

type OldUsage struct {
	LustreVolume        string
	PI                  sql.NullString
	UnixGroup           string
	Used                int64
	Quota               int64
	LastModified        float32
	ArchivedDirectories string
	Date                time.Time
	IsHumgen            int
}

type KeyData struct {
	PIs, Volumes map[string]int
	UnixGroups   map[string]map[bool]int
}

type DBCreds struct {
	Host string `yaml:"HOST"`
	Port int    `yaml:"PORT"`
	Name string `yaml:"NAME"`
	User string `yaml:"USER"`
	Pass string `yaml:"PASS"`
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

	configYamlFile, err := os.ReadFile("config.yml")
	if err != nil {
		panic(err)
	}

	var db_creds DBCreds
	err = yaml.Unmarshal(configYamlFile, &db_creds)

	if err != nil {
		panic(err)
	}

	// DB Connections
	fmt.Println("Connecting to DB")
	db, err := sql.Open(
		"mysql",
		fmt.Sprintf(
			"%s:%s@tcp(%s:%v)/%s",
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
	fmt.Println("PIs")
	// Quite a few entries here are NULL, which we need to deal with,
	// because there's still current NULL entries
	// When we do the main data transfer, it can be sorted there
	// Here, we'll just pull the NOT NULL ones
	results, err = db.Query("SELECT DISTINCT PI from hgi_lustre_usage.lustre_usage WHERE PI IS NOT NULL")
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

		_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.pi (pi_name) VALUES (?);", pi)

		if err != nil {
			panic(err)
		}

		var new_pi_id int

		err = db.QueryRow("SELECT pi_id FROM hgi_lustre_usage_new.pi WHERE pi_name = ?", pi).Scan(&new_pi_id)
		if err != nil {
			panic(err)
		}

		pis[pi] = new_pi_id

	}

	// Unixgroups
	fmt.Println("Unix Groups")
	// This is only NULL for a few old records, so its safe to ignore it
	results, err = db.Query("SELECT DISTINCT `Unix Group`, `IsHumgen` from hgi_lustre_usage.lustre_usage WHERE `Unix Group` IS NOT NULL")
	if err != nil {
		panic(err)
	}

	unixgroups := make(map[string]map[bool]int)

	for results.Next() {
		var unixgroup string
		var isHumgen int
		err = results.Scan(&unixgroup, &isHumgen)

		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (?, ?);", unixgroup, isHumgen == 1)

		if err != nil {
			panic(err)
		}

		var new_group_id int

		err = db.QueryRow("SELECT group_id FROM hgi_lustre_usage_new.unix_group WHERE group_name = ?", unixgroup).Scan(&new_group_id)
		if err != nil {
			panic(err)
		}

		if unixgroups[unixgroup] == nil {
			unixgroups[unixgroup] = make(map[bool]int)
		}

		unixgroups[unixgroup][isHumgen == 1] = new_group_id
	}

	// Lustre Volumes
	fmt.Println("Lustre Volumes")
	// This is never null :)
	results, err = db.Query("SELECT DISTINCT `Lustre Volume` from hgi_lustre_usage.lustre_usage")
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

		_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (?);", volume)

		if err != nil {
			panic(err)
		}

		var new_vol_id int

		err = db.QueryRow("SELECT volume_id FROM hgi_lustre_usage_new.volume WHERE scratch_disk = ?", volume).Scan(&new_vol_id)
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
	fmt.Println("Processing Main Data")
	jobs := make(chan OldUsage, NUM_WORKERS)
	var wg sync.WaitGroup

	wg.Add(NUM_WORKERS)
	for i := 0; i < NUM_WORKERS; i++ {
		go transfer_worker(jobs, wg, keyData, db)
	}

	// Check the query
	bigQuery, err := db.Query("SELECT (`Lustre Volume`, PI, `Unix Group`, Used, Quota, `Last Modified`, `Archived Directories`, Date, `Is Humgen`) FROM lustre_usage WHERE date > 2021-08-01 AND `Unix Group` IS NOT NULL")

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
