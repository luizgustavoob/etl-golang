FROM golang:alpine

# Setar o diretório de trabalho
WORKDIR /go/src/import-file-nwy

# Instalar git. Precisa para instalar a dependência github.com/lib/pq depois
RUN apk update && apk add --no-cache git

# Copia tudo do diretório para dentro do container
# COPY . . 
ADD . /go/src/import-file-nwy

USER root

# Formata a base de dados, atribuindo através de uma expressão regular um delimitador mais fácil para se trabalhar
RUN sed 's/  */\|/g' /go/src/import-file-nwy/base_teste.txt > /go/src/import-file-nwy/base_formatada.txt

RUN adduser -D -g '' postgres
RUN chown -R postgres /root/
RUN chown -R postgres /go/src/import-file-nwy/base_formatada.txt
RUN chown -R postgres base_formatada.txt

# Baixa a dependência para lidar com o banco postgres
RUN go get -u github.com/lib/pq

# Instala os pacotes da aplicação (main, database)
RUN go install /go/src/import-file-nwy/db
# RUN go install /go/src/import-file-nwy/main.go

# Executa
CMD go run /go/src/import-file-nwy/main.go

# Expoe a porta 8085 do container
EXPOSE 8085

# Executa
# CMD $WORKDIR/main