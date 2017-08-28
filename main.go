package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db := setDb()
	DumpTables(db)
	defer db.Close()
}

func DbVars() (user, password, host, port, database string) {
	user = os.Getenv("DB_USER")
	password = os.Getenv("DB_PASSWORD")
	host = os.Getenv("DB_HOST")
	port = os.Getenv("DB_PORT")
	database = os.Getenv("DB_DATABASE")
	return
}

func setDb() *sql.DB {
	user, password, host, port, database := DbVars()
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		user, password, host, port, database)
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func DumpTables(db *sql.DB) {
	q := `SELECT id, title, description,
	 	  		logo_url, web_site_url, data_url,
	 	  		refresh_period_days, name_strings_count,
	 	  		data_hash, unique_names_count, created_at,
	 	  		updated_at
	 	  	FROM data_sources`
	handleDataSource(RunQuery(db, q))
}

func RunQuery(db *sql.DB, q string) *sql.Rows {
	rows, err := db.Query(q)
	if err != nil {
		log.Fatal(err)
	}
	return rows
}

func handleDataSource(rows *sql.Rows) {
	var id int
	var title string
	var refresh_period_days, name_strings_count sql.NullInt64
	var unique_names_count sql.NullInt64
	var description, logo_url, web_site_url sql.NullString
	var data_url, data_hash sql.NullString
	var created_at, updated_at time.Time

	w := csv.NewWriter(os.Stdout)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &title, &description, &logo_url, &web_site_url,
			&data_url, &refresh_period_days, &name_strings_count, &data_hash,
			&unique_names_count, &created_at, &updated_at)
		if err != nil {
			log.Fatal(err)
		}
		csvRow := []string{strconv.Itoa(id), title, description.String,
			logo_url.String, web_site_url.String, data_url.String,
			strconv.Itoa(int(refresh_period_days.Int64)),
			strconv.Itoa(int(name_strings_count.Int64)), data_hash.String,
			strconv.Itoa(int(unique_names_count.Int64)),
			created_at.String(), updated_at.String()}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
}
