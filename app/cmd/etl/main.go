package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/etl-golang/app/domain"
	"github.com/etl-golang/app/infrastructure/client"
	postgres "github.com/etl-golang/app/infrastructure/storage"
)

var (
	etl      domain.ETL
	db       *sql.DB
	filename *string
)

func init() {
	filename = flag.String("f", "", "set a file to import")
	flag.Parse()
}

func main() {
	if *filename == "" {
		fmt.Println("\"File\" param not be empty")
		return
	}

	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		fmt.Println("ERROR: File not found.")
		return
	}

	db, err := postgres.NewPostgresDB(getDatabase())
	if err != nil {
		return
	}

	etl = client.NewETLClient(db)

	fmt.Println("Início", time.Now().Format("02/01/2006 15:04:05"))

	err = etl.InsertRawData(*filename)
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		return
	}

	err = etl.InsertCleanData()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		return
	}

	etl.RecordCount()

	fmt.Println("Fim", time.Now().Format("02/01/2006 15:04:05"))
}

func getDatabase() string {
	database := os.Getenv("DATABASE")
	if database == "" {
		return "host=localhost port=5439 user=postgres password=postgres dbname=nwy sslmode=disable"
	}
	return database
}
