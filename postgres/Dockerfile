FROM postgres:13-alpine
# ENV POSTGRES_USER postgres
# ENV POSTGRES_PASSWORD postgres
# ENV POSTGRES_DB nwy
# Copia os scripts de criação de tabela
COPY main.sql /docker-entrypoint-initdb.d
# Expoe a porta
EXPOSE 5432