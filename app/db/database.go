package database

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/lib/pq"
)

var mydb *sql.DB

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

const sqlCleanData = "insert into dados_limpos " +
	" (cpf, private, incompleto, ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra) " +
	" (select replace(replace(replace(db.cpf, '.', ''), '/', ''), '-', '') as cpf, " +
	"				cast(db.private as integer) as private, " +
	"				cast(db.incompleto as integer) as incompleto, " +
	"				(case db.ultima_compra " +
	"					when 'NULL' then null " +
	"					else cast(db.ultima_compra as date) " +
	"		end) as ultima_compra, " +
	"		(case db.ticket_medio " +
	"					when 'NULL' then null " +
	"					else cast(replace(db.ticket_medio, ',', '.') as decimal(10, 2)) " +
	"		end) as ticket_medio, " +
	"		(case db.ticket_ultima_compra " +
	"					when 'NULL' then null " +
	"					else cast(replace(db.ticket_ultima_compra, ',', '.') as decimal(10, 2)) " +
	"		end) as ticket_ultima_compra, " +
	"		(case db.loja_mais_frequente " +
	"					when 'NULL' then null " +
	"					else replace(replace(replace(db.loja_mais_frequente, '.', ''), '/', ''), '-', '') " +
	"		end) as loja_mais_frequente, " +
	"		(case db.loja_ultima_compra " +
	"					when 'NULL' then null " +
	"				else replace(replace(replace(db.loja_ultima_compra, '.', ''), '/', ''), '-', '') " +
	"		end) as loja_ultima_compra " +
	" from dados_brutos db)"

const sqlSelectCount = "select count(cpf) as qtde from dados_limpos"

func checkErr(err error) {
	if err != nil {
		fmt.Println("Erro:", err.Error())
		panic(err)
	}
}

// OpenDB abre o banco de dados
func OpenDB() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE"))
	checkErr(err)

	mydb = db

	errPing := mydb.Ping()
	checkErr(errPing)
}

// CloseDB fecha a conexão com o banco de dados
func CloseDB() {
	mydb.Close()
}

// InsertRawData insere os dados brutos, sem verificações
func InsertRawData(file string) {
	// abre o arquivo "full"
	content, err := ioutil.ReadFile(file)
	checkErr(err)

	// abre transação do banco
	txn, errTxn := mydb.Begin()
	checkErr(errTxn)

	// prepara o COPY FROM
	stmt, errCopyIn := txn.Prepare(pq.CopyIn("dados_brutos", "cpf", "private", "incompleto", "ultima_compra",
		"ticket_medio", "ticket_ultima_compra", "loja_mais_frequente", "loja_ultima_compra"))
	checkErr(errCopyIn)

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
			checkErr(errCopyIn)
		}
	}

	// fecha statement
	errStmt := stmt.Close()
	checkErr(errStmt)

	// commita transação
	errCmt := txn.Commit()
	checkErr(errCmt)
}

// InsertCleanData a partir da tabela de dados brutos, insere os dados limpos, higienizados
func InsertCleanData() {
	_, err := mydb.Exec(sqlCleanData)
	checkErr(err)
}

// SelectCount realiza a contagem de registros na tabela de dados limpos
func SelectCount() {
	rows, err := mydb.Query(sqlSelectCount)
	checkErr(err)

	for rows.Next() {
		var rowsAffected int64
		rows.Scan(&rowsAffected)
		fmt.Println("Rows affected:", rowsAffected)
	}

	rows.Close()
}
