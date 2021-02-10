package client

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/lib/pq"
)

const (
	colunaCpf                = 0
	colunaPrivate            = 1
	colunaIncompleto         = 2
	colunaUltimaCompra       = 3
	colunaTicketMedio        = 4
	colunaTicketUltimaCompra = 5
	colunaLojaMaisFrequente  = 6
	colunaLojaUltimaCompra   = 7
)

type etlClient struct {
	db *sql.DB
}

func NewETLClient(db *sql.DB) *etlClient {
	return &etlClient{db}
}

func (e *etlClient) InsertRawData(file string) (err error) {
	// abre o arquivo "full"
	content, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	// abre transação do banco
	txn, err := e.db.Begin()
	if err != nil {
		return
	}

	// prepara o COPY FROM
	stmt, err := txn.Prepare(pq.CopyIn("dados_brutos", "cpf", "private", "incompleto", "ultima_compra",
		"ticket_medio", "ticket_ultima_compra", "loja_mais_frequente", "loja_ultima_compra"))
	if err != nil {
		return
	}

	// pega todas as linhas do arquivo
	lines := strings.Split(string(content), "\n")
	count := 0
	for _, line := range lines {
		count++
		// ignora header
		if count == 1 {
			continue
		}

		// pega as colunas do arquivo. vai ser cada field no bd
		columns := strings.Split(strings.TrimSpace(line), "|")

		// a última linha do arquivo tá em branco, portanto o split não vai retornar colunas
		if len(columns) > 0 {
			_, err = stmt.Exec(columns[colunaCpf], columns[colunaPrivate], columns[colunaIncompleto], columns[colunaUltimaCompra],
				columns[colunaTicketMedio], columns[colunaTicketUltimaCompra], columns[colunaLojaMaisFrequente], columns[colunaLojaUltimaCompra])
			if err != nil {
				return
			}
		}
	}

	// fecha statement
	err = stmt.Close()
	if err != nil {
		return
	}

	// commita transação
	err = txn.Commit()
	if err != nil {
		return
	}

	return nil
}

func (e *etlClient) InsertCleanData() (err error) {
	_, err = e.db.Exec(`
		insert into dados_limpos 
		(cpf, private, incompleto, ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra) 
		(select replace(replace(replace(db.cpf, '.', ''), '/', ''), '-', '') as cpf, 
						cast(db.private as integer) as private, 
						cast(db.incompleto as integer) as incompleto, 
						(case db.ultima_compra 
							when 'NULL' then null 
							else cast(db.ultima_compra as date) 
				end) as ultima_compra, 
				(case db.ticket_medio 
							when 'NULL' then null 
							else cast(replace(db.ticket_medio, ',', '.') as decimal(10, 2)) 
				end) as ticket_medio, 
				(case db.ticket_ultima_compra 
							when 'NULL' then null 
							else cast(replace(db.ticket_ultima_compra, ',', '.') as decimal(10, 2)) 
				end) as ticket_ultima_compra, 
				(case db.loja_mais_frequente 
							when 'NULL' then null 
							else replace(replace(replace(db.loja_mais_frequente, '.', ''), '/', ''), '-', '') 
				end) as loja_mais_frequente, 
				(case db.loja_ultima_compra 
							when 'NULL' then null 
						else replace(replace(replace(db.loja_ultima_compra, '.', ''), '/', ''), '-', '') 
				end) as loja_ultima_compra 
		from dados_brutos db)`)

	return
}

func (e *etlClient) RecordCount() {
	rows, err := e.db.Query("select count(cpf) as qtde from dados_limpos")
	if err != nil {
		fmt.Printf("ERROR: %s\n", err)
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var rowsAffected int64
		rows.Scan(&rowsAffected)
		fmt.Println("Rows affected:", rowsAffected)
	}
}
