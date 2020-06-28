# Importação de arquivo

Essa aplicação propõe uma solução para importar um arquivo .txt com quase 50.000 linhas no menor tempo possível. Para isso, utiliza a linguagem Go e o banco de dados PostgreSQL.

## Pré-requisitos
* [Docker](https://www.docker.com/)

## Execução

Após baixar esse repositório, você deverá acessá-lo via terminal (até a pasta raiz). Na sequência, deverá executar o comando
```
docker-compose up -d postgres
```
para que o banco de dados fique disponível.

Ao final desse procedimento, executar o comando
```
docker-compose up go
```
e a aplicação fará a importação dos dados. 

Nos logs do terminal serão apresentadas algumas informações, destacando a hora em que o processo se iniciou e encerrou, bem como o número de registros importados.
