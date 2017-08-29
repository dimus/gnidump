package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db := setDb()
	prepare()
	DumpTables(db)
	defer db.Close()
}

func prepare() {
	gniPath := "/tmp/gni_mysql"
	if _, err := os.Stat(gniPath); os.IsNotExist(err) {
		os.Mkdir(gniPath, 0777)
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
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
	check(err)
	return db
}

func DumpTables(db *sql.DB) {
	dumpTableDataSources(db)
	dumpTableNameStrings(db)
	dumpTableNameStringIndices(db)
	dumpTableVernacularStrings(db)
	dumpTableVernacularStringIndices(db)
}

func dumpTableVernacularStringIndices(db *sql.DB) {
	log.Print("Create vernacular_string_indices.csv")
	q := `SELECT data_source_id, taxon_id,
					vernacular_string_id, language, locality,
					country_code
					FROM vernacular_string_indices`

	handleVernacularStringIndices(RunQuery(db, q))
}
func handleVernacularStringIndices(rows *sql.Rows) {
	var data_source_id, taxon_id, vernacular_string_id string
	var language, locality, country_code sql.NullString
	file := csvFile("vernacular_string_indices")
	defer file.Close()
	w := csv.NewWriter(file)
	err := w.Write([]string{"data_source_id", "taxon_id", "vernacular_string_id",
		"language", "locality", "country_code"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&data_source_id, &taxon_id, &vernacular_string_id,
			&language, &locality, &country_code)
		check(err)
		csvRow := []string{data_source_id, taxon_id, vernacular_string_id,
			language.String, locality.String, country_code.String}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	file.Sync()
}

func dumpTableVernacularStrings(db *sql.DB) {
	log.Print("Create vernacular_strings.csv")
	q := "SELECT id, name FROM vernacular_strings"
	handleVernacularStrings(RunQuery(db, q))
}

func handleVernacularStrings(rows *sql.Rows) {
	var id string
	var name string
	file := csvFile("vernacular_strings")
	defer file.Close()
	w := csv.NewWriter(file)
	err := w.Write([]string{"id", "name"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &name)
		check(err)
		csvRow := []string{id, name}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	file.Sync()
}

func dumpTableNameStringIndices(db *sql.DB) {
	log.Print("Create name_string_indices.csv")
	q := `SELECT data_source_id, name_string_id,
					url, taxon_id, global_id, local_id,
					nomenclatural_code_id, rank,
					accepted_taxon_id, classification_path,
					classification_path_ids,
					classification_path_ranks
					FROM name_string_indices`
	handleNameStringIndices(RunQuery(db, q))
}

func handleNameStringIndices(rows *sql.Rows) {
	var data_source_id, name_string_id, taxon_id string
	var url, global_id, local_id, nomenclatural_code_id, rank sql.NullString
	var accepted_taxon_id sql.NullString
	var classification_path, classification_path_ids sql.NullString
	var classification_path_ranks sql.NullString
	file := csvFile("name_string_indices")
	defer file.Close()
	w := csv.NewWriter(file)
	err := w.Write([]string{"data_source_id", "name_string_id", "url",
		"taxon_id", "global_id", "local_id", "nomenclatural_code_id", "rank",
		"accepted_taxon_id", "classification_path", "classification_path_ids",
		"classification_path_ranks"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&data_source_id, &name_string_id, &url, &taxon_id,
			&global_id, &local_id, &nomenclatural_code_id, &rank, &accepted_taxon_id,
			&classification_path, &classification_path_ids,
			&classification_path_ranks)
		check(err)
		csvRow := []string{data_source_id, name_string_id, url.String, taxon_id,
			global_id.String, local_id.String, nomenclatural_code_id.String,
			rank.String, accepted_taxon_id.String, classification_path.String,
			classification_path_ids.String, classification_path_ranks.String}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	file.Sync()
}

func dumpTableNameStrings(db *sql.DB) {
	log.Print("Create name_strings.csv")
	q := `SELECT id, name
					FROM name_strings`
	handleNameStrings(RunQuery(db, q))
}

func handleNameStrings(rows *sql.Rows) {
	var id string
	var name string
	file := csvFile("name_strings")
	defer file.Close()
	w := csv.NewWriter(file)
	err := w.Write([]string{"id", "name"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &name)
		check(err)
		name := strings.Replace(name, "\u0000", "", -1)
		csvRow := []string{id, name}

		if err := w.Write(csvRow); err != nil {
			log.Fatal(err)
		}
	}
	w.Flush()
	file.Sync()
}

func dumpTableDataSources(db *sql.DB) {
	log.Print("Create data_sources.csv")
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
	check(err)
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
	file := csvFile("data_sources")
	defer file.Close()
	w := csv.NewWriter(file)

	err := w.Write([]string{"id", "title", "description",
		"logo_url", "web_site_url", "data_url",
		"refresh_period_days", "name_strings_count",
		"data_hash", "unique_names_count", "created_at",
		"updated_at"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &title, &description, &logo_url, &web_site_url,
			&data_url, &refresh_period_days, &name_strings_count, &data_hash,
			&unique_names_count, &created_at, &updated_at)
		check(err)
		csvRow := []string{strconv.Itoa(id), title, description.String,
			logo_url.String, web_site_url.String, data_url.String,
			strconv.Itoa(int(refresh_period_days.Int64)),
			strconv.Itoa(int(name_strings_count.Int64)), data_hash.String,
			strconv.Itoa(int(unique_names_count.Int64)),
			created_at.String(), updated_at.String()}

		check(w.Write(csvRow))
	}
	w.Flush()
	file.Sync()
}

func csvFile(f string) *os.File {
	file, err := os.Create("/tmp/gni_mysql/" + f + ".csv")
	check(err)
	return file
}
