package main

import (
	"fmt"
	database "import-file-nwy/db"
	"time"
)

const fileName = "base_formatada.txt"

func main() {
	fmt.Println("Início", time.Now().Format("02/01/2006 15:04:05"))

	database.OpenDB()
	database.InsertRawData(fileName)
	database.InsertCleanData()
	database.SelectCount()
	database.CloseDB()

	fmt.Println("Fim", time.Now().Format("02/01/2006 15:04:05"))
}
