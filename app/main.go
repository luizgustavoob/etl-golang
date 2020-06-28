package main

import (
	"fmt"
	database "import-file-nwy/db"
	"os"
	"path/filepath"
	"time"

	_ "github.com/lib/pq"
)

const fileName = "base_formatada.txt"

func main() {
	fmt.Println("Início", time.Now().Format("02/01/2006 15:04:05"))

	rootPathFile, _ := os.UserHomeDir()
	pathFile, _ := filepath.Abs(fileName)
	rootPathFile += pathFile

	database.OpenDB()
	database.InsertRawData(rootPathFile)
	database.InsertCleanData()
	database.SelectCount()
	database.CloseDB()

	fmt.Println("Fim", time.Now().Format("02/01/2006 15:04:05"))
}
