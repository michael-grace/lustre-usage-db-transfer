package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v2"
)

const (
	NUM_WORKERS = 100
)

type OldRecord struct {
	Project      string
	Directory    string
	Volume       int
	Files        int
	Total        float32
	BAM          float32
	CRAM         float32
	VCF          float32
	PEDBED       float32
	LastModified float32
	PI           string // PI and UnixGroup don't use NULL, they use "-"
	UnixGroup    string
}

type KeyData struct {
	PIs, Volumes map[string]int
	UnixGroups   map[string]map[int]int
}

type DBCreds struct {
	Host string `yaml:"HOST"`
	Port int    `yaml:"PORT"`
	Name string `yaml:"NAME"`
	User string `yaml:"USER"`
	Pass string `yaml:"PASS"`
}

func main() {
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

	// We need to make sure the lookup table has all the PIs we need,
	// and not adding "-" as a PI, that _should_ be represented as NULL

	// First, get what already exists
	results, err = db.Query("SELECT * FROM hgi_lustre_usage_new.pi")
	if err != nil {
		panic(err)
	}

	pis := make(map[string]int)
	for results.Next() {
		var pi_id int
		var pi_name string
		err = results.Scan(&pi_id, &pi_name)

		if err != nil {
			panic(err)
		}

		pis[pi_name] = pi_id
	}

	// Now add new ones
	results, err = db.Query("SELECT DISTINCT PI from hgi_lustre_usage.spaceman")
	if err != nil {
		panic(err)
	}

	for results.Next() {
		var pi_name string
		err = results.Scan(&pi_name)

		if err != nil {
			panic(err)
		}

		if pi_name == "-" {
			continue
		}

		if _, ok := pis[pi_name]; !ok {
			_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.pi (pi_name) VALUES (?);", pi_name)

			if err != nil {
				panic(err)
			}

			var new_pi_id int
			err = db.QueryRow("SELECT pi_id FROM hgi_lustre_usage_new.pi WHERE pi_name = ?;", pi_name).Scan(&new_pi_id)

			if err != nil {
				panic(err)
			}

			pis[pi_name] = new_pi_id
		}
	}

	// Unixgroups
	fmt.Println("Unixgroups")

	// What we've already got
	results, err = db.Query("SELECT * FROM hgi_lustre_usage_new.unix_group")
	if err != nil {
		panic(err)
	}

	groups := make(map[string]map[int]int)
	for results.Next() {
		var group_id int
		var group_name string
		var isHumgen int

		err = results.Scan(&group_id, &group_name, &isHumgen)

		if err != nil {
			panic(err)
		}

		if groups[group_name] == nil {
			groups[group_name] = make(map[int]int)
		}

		groups[group_name][isHumgen] = group_id

	}

	// Adding anything new
	// Spaceman doesn't care about whether the group is part of
	// humgen or not, unlike weaver, so we'll just say everybody is

	results, err = db.Query("SELECT DISTINCT `Unix Group` from hgi_lustre_usage.spaceman")
	if err != nil {
		panic(err)
	}

	for results.Next() {
		var group_name string
		err = results.Scan(&group_name)

		if err != nil {
			panic(err)
		}

		if group_name == "-" {
			continue
		}

		if _, ok := groups[group_name][1]; !ok {
			_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (?, 1)", group_name)

			if err != nil {
				panic(err)
			}

			var new_group_id int

			err = db.QueryRow("SELECT group_id FROM hgi_lustre_usage_new.unix_group WHERE group_name = ? AND is_humgen = 1", group_name).Scan(&new_group_id)

			if err != nil {
				panic(err)
			}

			if groups[group_name] == nil {
				groups[group_name] = make(map[int]int)
			}

			groups[group_name][1] = new_group_id
		}

	}

	// Lustre Volumes
	fmt.Println("Lustre Volumes")

	// First, get what already exists
	results, err = db.Query("SELECT * FROM hgi_lustre_usage_new.volume")
	if err != nil {
		panic(err)
	}

	volumes := make(map[string]int)
	for results.Next() {
		var volume_id int
		var volume_name string
		err = results.Scan(&volume_id, &volume_name)

		if err != nil {
			panic(err)
		}

		volumes[volume_name] = volume_id
	}

	// Now, add new ones (however unlikely)
	results, err = db.Query("SELECT DISTINCT Volume FROM hgi_lustre_usage.spaceman")
	if err != nil {
		panic(err)
	}

	for results.Next() {
		// They are stored in spaceman as just numbers, we need to convert them to scratch disks
		var volume_number int
		err = results.Scan(&volume_number)
		if err != nil {
			panic(err)
		}

		volume_name := fmt.Sprintf("scratch%v", volume_number)
		if _, ok := volumes[volume_name]; !ok {
			_, err = db.Exec("INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (?)", volume_name)
			if err != nil {
				panic(err)
			}

			var new_vol_id int
			err = db.QueryRow("SELECT volume_id FROM hgi_lustre_usage_new.volume WHERE scratch_disk = ?", volume_name).Scan(&new_vol_id)
			if err != nil {
				panic(err)
			}

			volumes[volume_name] = new_vol_id
		}
	}

	// OK, lets worry about transferring some actual data
	keyData := KeyData{
		PIs:        pis,
		Volumes:    volumes,
		UnixGroups: groups,
	}

	fmt.Println("Processing Main Data")
	jobs := make(chan OldRecord, NUM_WORKERS)
	var wg sync.WaitGroup

	wg.Add(NUM_WORKERS)
	for i := 0; i < NUM_WORKERS; i++ {
		go transfer_worker(jobs, &wg, keyData, db)
	}

	bigQuery, err := db.Query("SELECT Project, Directory, Volume, Files, Total, BAM, CRAM, VCF, PEDBED, `Last Modified (days)`, PI, `Unix Group` FROM hgi_lustre_usage.spaceman")

	if err != nil {
		panic(err)
	}

	for bigQuery.Next() {
		var dataPoint OldRecord
		err = bigQuery.Scan(
			&dataPoint.Project,
			&dataPoint.Directory,
			&dataPoint.Volume,
			&dataPoint.Files,
			&dataPoint.Total,
			&dataPoint.BAM,
			&dataPoint.CRAM,
			&dataPoint.VCF,
			&dataPoint.PEDBED,
			&dataPoint.LastModified,
			&dataPoint.PI,
			&dataPoint.UnixGroup,
		)

		if err != nil {
			panic(err)
		}

		jobs <- dataPoint
	}
	close(jobs)

	wg.Wait()
}
