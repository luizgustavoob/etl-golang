FROM golang:alpine
WORKDIR /go/src/github.com/etl-golang/app
COPY . .
RUN chmod +x wait-for-it.sh
RUN sed 's/  */\|/g' base_teste.txt > base_formatada.txt
RUN apk update && apk add --no-cache git && apk add bash && go get -u github.com/lib/pq
EXPOSE 8085