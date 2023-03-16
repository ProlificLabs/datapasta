package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sqlclone"
)

func main() {

	// define with which row(s) the cloning should start and which table(s) should be ignored during the process
	download_options, err := sqlclone.NewDownloadOptions(
		sqlclone.Include("company", "id", "3"),
		sqlclone.DontRecurse("person"),
		sqlclone.DontRecurse("company"),
	)
	if err != nil {
		log.Fatal(err)
	}

	fromDB, err := sqlclone.NewPostgresAdapter("localhost", 5432, "baay", "deneme", "db_sqlclone")
	if err != nil {
		log.Fatal(err)
	}

	dump, err := sqlclone.Download(fromDB, download_options)
	if err != nil {
		log.Fatal(err)
	}

	dataBytes, err := json.Marshal(dump)
	if err != nil {
		log.Fatal(err)
	}

	newData := make(sqlclone.DatabaseDump)
	err = json.Unmarshal(dataBytes, &newData)
	if err != nil {
		log.Fatal(err)
	}

	toDB, err := sqlclone.NewPostgresAdapter("localhost", 5432, "baay", "deneme", "db_sqlclone_to")
	if err != nil {
		log.Fatal(err)
	}

	m, err := sqlclone.Upload(toDB, newData)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(m)

}
