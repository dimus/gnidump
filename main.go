package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	user, password, host, port, database := dbVars()
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		user, password, host, port, database)
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func dbVars() (user, password, host, port, database string) {
	user = os.Getenv("DB_USER")
	password = os.Getenv("DB_PASSWORD")
	host = os.Getenv("DB_HOST")
	port = os.Getenv("DB_PORT")
	database = os.Getenv("DB_DATABASE")
	return
}
