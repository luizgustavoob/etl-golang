# ETL - Golang + PostgreSQL

Essa aplicação propõe uma solução para importar um arquivo .txt com quase 50.000 linhas no menor tempo possível. Para isso, utiliza a linguagem Go e o banco de dados PostgreSQL.

## Pré-requisitos
* [Docker](https://www.docker.com/)

## Execução

Após baixar esse repositório, você deverá acessá-lo via terminal desde a pasta raiz (Dica: no diretório onde se encontra o arquivo **docker-compose.yml**). Na sequência, deverá executar o comando
```
docker-compose up
```
e a aplicação fará a importação dos dados. 

Nos logs do terminal serão apresentadas algumas informações, destacando a hora em que o processo se iniciou e encerrou, bem como o número de registros importados.

Como a aplicação foi gerada no formato executável, após o comando que sobe o container do Go ser executado, o container irá morrer. Já o container do PostgreSQL ficará no ar, podendo ser acessado por algum SGBD (no arquivo **docker-compose.yml** tem as credenciais do banco).