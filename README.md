# Lustre Usage Database Transfer Script

For transferring lustre usage data from the old schema to the new one. (Schema defined in `schema.sql`, though this script assumes it already exists).

- Copy `config.example.yml` to `config.yml` and fill out the DB credentials. The DB user needs both read and write access.
- Change the earliest date for it to copy data from in the SQL query on line 217 of `transferer.go`, and the number of workers to create in line 13.
- Run `go run *.go` 