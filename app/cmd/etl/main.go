package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/etl-golang/app/domain"
	"github.com/etl-golang/app/infrastructure/client"
	postgres "github.com/etl-golang/app/infrastructure/storage"
)

var (
	etl domain.ETL
	db  *sql.DB
)

const filename = "base_formatada.txt"

func main() {

	// for {
	// 	time.Sleep(10 * time.Second)

	// }
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		fmt.Println("ERROR: File not found.")
		return
	}

	db, err := postgres.NewPostgresDB(os.Getenv("DATABASE"))
	if err != nil {
		return
	}

	etl = client.NewETLClient(db)

	fmt.Println("Início", time.Now().Format("02/01/2006 15:04:05"))

	err = etl.InsertRawData(filename)
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
