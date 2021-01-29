package main

import (
	"fmt"
	"os"
	"time"

	database "github.com/etl-golang/app/db"
)

const fileName = "base_formatada.txt"

func main() {
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		fmt.Println("ERROR: File not found.")
		return
	}

	fmt.Println("Início", time.Now().Format("02/01/2006 15:04:05"))

	database.OpenDB()
	database.InsertRawData(fileName)
	database.InsertCleanData()
	database.SelectCount()
	database.CloseDB()

	fmt.Println("Fim", time.Now().Format("02/01/2006 15:04:05"))
}
