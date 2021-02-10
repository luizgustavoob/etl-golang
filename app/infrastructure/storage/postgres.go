package postgres

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

type postgresDB struct {
	db *sql.DB
}

func NewPostgresDB(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %s", err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to check connection on PostgreSQL: %s", err)
		return nil, err
	}

	log.Println("PostgreSQL connection ok!")
	return db, nil
}
