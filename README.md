# Desafio ETL

## Descrição
Este projeto tem o objetivo de apresentar uma solução para processamento de dados de arquivos .txt, trabalhando os dados recebidos no arquivo para que sejam armazenados em um banco de dados PostgreSQL. O serviço foi desenvolvido em GO. 


## Funcionalidades
- Recebe arquivo de dados .TXT
- Inserção dos dados no PostgreSQL
- Realizaa higienização e padronização dos dados
- Valida dados de identificação (CNPJ e CPF)

## Requisitos
- Go 1.20+
- PostgreSQL
- Dependências Go:
  - ```github.com/lib/pq```

## Como Executar
-  Crie a tabela no seu banco de dados local PostgreSQL:
  ```
  CREATE TABLE base_testes (
    id SERIAL PRIMARY KEY,
    CPF VARCHAR(100),
    PRIVATE INT,
    INCOMPLETO INT,
    DATA_ULTIMA_COMPRA DATE,
    TICKET_MEDIO DOUBLE PRECISION,
    TICKET_ULTIMA_COMPRA DOUBLE PRECISION,
    LOJA_MAIS_FREQUENTE VARCHAR(100),
    LOJA_ULTIMA_COMPRA VARCHAR(100)
);
```
- Na pasta raiz do projeto, instale as dependências: ```go mod tidy```
- Insira o arquivo de dados na pasta raiz
- Informe o nome do arquivo que será manipulado em "filePath"
- Execute a aplicação: go run main.go



