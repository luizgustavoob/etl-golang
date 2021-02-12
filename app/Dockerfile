FROM golang:alpine
WORKDIR /go/src/github.com/etl-golang/app
COPY . .
RUN apk update 
RUN apk add --no-cache git 
RUN apk add bash  
RUN go get -u github.com/lib/pq
EXPOSE 8085