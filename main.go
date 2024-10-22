package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func ConnectDB() *sql.DB {
	connStr := "user=postgres password=postgres dbname=postgres sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func processWithTransaction(db *sql.DB, scanner *bufio.Scanner) error {
	start := time.Now()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("erro ao iniciar a transação: %v", err)
	}

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.Join(strings.Fields(line), " ")
		row := strings.Split(line, " ")

		if len(row) != 8 {
			fmt.Println("Linha inválida:", row)
			continue
		}

		for j := range row {
			if strings.TrimSpace(row[j]) == "NULL" {
				row[j] = ""
			}
		}

		cpf := sanitizeCPF(row[0])
		dataUltimaCompra := row[3]
		ticketMedio := sanitizeNumber(row[4])
		ticketUltimaCompra := sanitizeNumber(row[5])
		lojaFreq := sanitizeCNPJ(row[6])
		lojaUltCompra := sanitizeCNPJ(row[7])

		if !ValidarCPF(cpf) {
			fmt.Println("CPF inválido:", cpf)
			continue
		}

		if lojaFreq != "" && !ValidarCNPJ(lojaFreq) {
			fmt.Println("CNPJ inválido:", lojaFreq)
			continue
		}
		if lojaUltCompra != "" && !ValidarCNPJ(lojaUltCompra) {
			fmt.Println("CNPJ inválido:", lojaUltCompra)
			continue
		}

		_, err := tx.Exec(
			`INSERT INTO base_testes 
			(CPF, PRIVATE, INCOMPLETO, DATA_ULTIMA_COMPRA, TICKET_MEDIO, TICKET_ULTIMA_COMPRA, LOJA_MAIS_FREQUENTE, LOJA_ULTIMA_COMPRA) 
			VALUES ($1, $2, $3, CASE WHEN $4 = '' THEN NULL ELSE TO_DATE($4, 'YYYY-MM-DD') END, 
			CASE WHEN $5 = '' THEN NULL ELSE CAST($5 AS DOUBLE PRECISION) END, 
			CASE WHEN $6 = '' THEN NULL ELSE CAST($6 AS DOUBLE PRECISION) END, $7, $8)`,
			cpf, row[1], row[2], dataUltimaCompra, ticketMedio, ticketUltimaCompra, lojaFreq, lojaUltCompra,
		)

		if err != nil {
			tx.Rollback()
			return fmt.Errorf("erro ao inserir no banco: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("erro ao commitar a transação: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("Dados processados com sucesso em %v\n", duration)
	return nil
}

func main() {
	db := ConnectDB()
	if db == nil {
		log.Fatal("Erro ao conectar no banco de dados")
		return
	}
	defer db.Close()

	filePath := "base_teste.txt" // informar nome do arquivo, que deve estar na pasta raiz
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Erro ao abrir o arquivo:", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		_ = scanner.Text()
	}

	if err := processWithTransaction(db, scanner); err != nil {
		fmt.Printf("Erro ao processar dados: %v\n", err)
	} else {
		fmt.Println("Processamento concluído com sucesso.")
	}
}

func sanitize(input string) string {
	return strings.ToUpper(strings.TrimSpace(input))
}

func sanitizeCPF(cpf string) string {
	re := regexp.MustCompile(`[^\d]`)
	return re.ReplaceAllString(cpf, "")
}

func sanitizeCNPJ(cnpj string) string {
	re := regexp.MustCompile(`[^\d]`)
	return re.ReplaceAllString(cnpj, "")
}

func sanitizeNumber(num string) string {
	return strings.Replace(num, ",", ".", -1)
}

func ValidarCPF(cpf string) bool {
	re := regexp.MustCompile(`^\d{11}$`)
	return re.MatchString(cpf)
}

func ValidarCNPJ(cnpj string) bool {
	re := regexp.MustCompile(`^\d{14}$`)
	return re.MatchString(cnpj)
}
