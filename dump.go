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
)

func prepare() {
	gniPath := "/tmp/gni_mysql"
	gnindexPath := "/tmp/gnindex_pg"
	badgerPath := "/tmp/badger"
	var err error
	if _, err := os.Stat(gniPath); os.IsNotExist(err) {
		os.Mkdir(gniPath, 0777)
	}
	check(err)
	if _, err := os.Stat(gnindexPath); os.IsNotExist(err) {
		os.Mkdir(gnindexPath, 0777)
	}
	check(err)
	if _, err := os.Stat(badgerPath); os.IsNotExist(err) {
		os.Mkdir(badgerPath, 0777)
	}
	check(err)
}

func dumpTables() {
	db := setDb()
	defer db.Close()
	dumpTableDataSources(db)
	dumpTableNameStrings(db)
	dumpTableNameStringIndices(db)
	dumpTableVernacularStrings(db)
	dumpTableVernacularStringIndices(db)
}

func envVars() map[string]string {
	env := make(map[string]string)
	env["user"] = os.Getenv("DB_USER")
	env["password"] = os.Getenv("DB_PASSWORD")
	env["host"] = os.Getenv("DB_HOST")
	env["port"] = os.Getenv("DB_PORT")
	env["database"] = os.Getenv("DB_DATABASE")
	env["workers"] = os.Getenv("WORKERS_NUMBER")
	env["parser_url"] = os.Getenv("PARSER_URL")
	return env
}

func setDb() *sql.DB {
	env := envVars()
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		env["user"], env["password"], env["host"], env["port"], env["database"])
	fmt.Println(url)
	db, err := sql.Open("mysql", url)
	check(err)
	return db
}

func dumpTableVernacularStringIndices(db *sql.DB) {
	log.Print("Create vernacular_string_indices.csv")
	q := `SELECT data_source_id, taxon_id,
					vernacular_string_id, language, locality,
					country_code
					FROM vernacular_string_indices`

	handleVernacularStringIndices(runQuery(db, q))
}
func handleVernacularStringIndices(rows *sql.Rows) {
	var dataSourceID, taxonID, vernacularStringID string
	var language, locality, countryCode sql.NullString
	file := csvFile("vernacular_string_indices")
	defer file.Close()
	w := csv.NewWriter(file)
	err := w.Write([]string{"data_source_id", "taxon_id", "vernacular_string_id",
		"language", "locality", "country_code"})
	check(err)

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&dataSourceID, &taxonID, &vernacularStringID,
			&language, &locality, &countryCode)
		check(err)
		csvRow := []string{dataSourceID, taxonID, vernacularStringID,
			language.String, locality.String, countryCode.String}

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
	handleVernacularStrings(runQuery(db, q))
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
	handleNameStringIndices(runQuery(db, q))
}

func handleNameStringIndices(rows *sql.Rows) {
	var dataSourceID, nameStringID, taxonID string
	var url, globalID, localID, nomenclaturalCodeID, rank sql.NullString
	var acceptedTaxonID sql.NullString
	var classificationPath, classificationPathIDs sql.NullString
	var classificationPathRanks sql.NullString
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
		err := rows.Scan(&dataSourceID, &nameStringID, &url, &taxonID,
			&globalID, &localID, &nomenclaturalCodeID, &rank, &acceptedTaxonID,
			&classificationPath, &classificationPathIDs,
			&classificationPathRanks)
		check(err)
		csvRow := []string{dataSourceID, nameStringID, url.String, taxonID,
			globalID.String, localID.String, nomenclaturalCodeID.String,
			rank.String, acceptedTaxonID.String, classificationPath.String,
			classificationPathIDs.String, classificationPathRanks.String}

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
	handleNameStrings(runQuery(db, q))
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

		err = w.Write(csvRow)
		check(err)
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
	handleDataSource(runQuery(db, q))
}

func runQuery(db *sql.DB, q string) *sql.Rows {
	rows, err := db.Query(q)
	check(err)
	return rows
}

func handleDataSource(rows *sql.Rows) {
	var id int
	var title string
	var refreshPeriodDays, nameStringsCount sql.NullInt64
	var uniqueNamesCount sql.NullInt64
	var description, logoURL, webSiteURL sql.NullString
	var dataURL, dataHash sql.NullString
	var createdAt, updatedAt time.Time
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
		err := rows.Scan(&id, &title, &description, &logoURL, &webSiteURL,
			&dataURL, &refreshPeriodDays, &nameStringsCount, &dataHash,
			&uniqueNamesCount, &createdAt, &updatedAt)
		check(err)
		created := createdAt.Format(time.RFC3339)
		updated := updatedAt.Format(time.RFC3339)
		csvRow := []string{strconv.Itoa(id), title, description.String,
			logoURL.String, webSiteURL.String, dataURL.String,
			strconv.Itoa(int(refreshPeriodDays.Int64)),
			strconv.Itoa(int(nameStringsCount.Int64)), dataHash.String,
			strconv.Itoa(int(uniqueNamesCount.Int64)),
			created, updated}

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
