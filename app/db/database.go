package database

import (
	"database/sql"
	"fmt"
	"os"
)

var mydb *sql.DB

const sqlCopyFrom = "copy %s from '%s' with delimiter '|' csv header"

const sqlCleanData = "insert into dados_limpos " +
	" (cpf, private, incompleto, ultima_compra, ticket_medio, ticket_ultima_compra, loja_mais_frequente, loja_ultima_compra) " +
	" (select replace(replace(db.cpf, '.', ''), '-', '') as cpf, " +
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

// OpenDB abre o banco de dados
func OpenDB() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE"))

	if err != nil {
		fmt.Println("Erro pra abrir bd", err.Error())
		panic(err)
	}

	mydb = db

	errPing := mydb.Ping()
	if errPing != nil {
		fmt.Println("Erro no ping", errPing.Error())
		panic(errPing)
	}
}

// CloseDB fecha a conexão com o banco de dados
func CloseDB() {
	mydb.Close()
}

// InsertRawData insere os dados brutos, sem verificações
func InsertRawData(file string) {
	_, err := mydb.Exec(fmt.Sprintf(sqlCopyFrom, "dados_brutos", file))

	if err != nil {
		panic(err.Error())
	}
}

// InsertCleanData a partir da tabela de dados brutos, insere os dados limpos, higienizados
func InsertCleanData() {
	_, err := mydb.Exec(sqlCleanData)

	if err != nil {
		panic(err.Error())
	}
}

// SelectCount realiza a contagem de registros na tabela de dados limpos
func SelectCount() {
	rows, err := mydb.Query(sqlSelectCount)

	if err != nil {
		panic(err.Error())
	}

	for rows.Next() {
		var rowsAffected int64
		rows.Scan(&rowsAffected)
		fmt.Println("Rows affected:", rowsAffected)
	}

	rows.Close()
}
