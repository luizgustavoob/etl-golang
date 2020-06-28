--Cria a tabela temporária que receberá a importação do arquivo. Nessa tabela não haverão regras, ela é apenas intermediária
create table public."dados_brutos" (
	cpf varchar(255),
	private varchar(255),
	incompleto varchar(255),
	ultima_compra varchar(255),
	ticket_medio varchar(255),
	ticket_ultima_compra varchar(255),
	loja_mais_frequente varchar(255),
	loja_ultima_compra varchar(255)
);

--Cria a tabela que receberá os dados higienizados do arquivo de importação. Essa tabela que deverá ser consumida
create table public."dados_limpos" (
	cpf varchar(20),
	private integer,
	incompleto integer,
	ultima_compra date,
	ticket_medio decimal(10, 2),
	ticket_ultima_compra decimal(10, 2),
	loja_mais_frequente varchar(25),
	loja_ultima_compra varchar(25),
	constraint pk_cpf primary key (cpf)
);