package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger"
	uuid "github.com/satori/go.uuid"
)

type ioJob struct {
	Writer string
	Row    []string
}

func createTables() {
	ioJobs := make(chan ioJob)

	var nameStringsWG sync.WaitGroup
	var indexWG sync.WaitGroup
	var ioWG sync.WaitGroup

	writers, files := initTables()
	defer closeWriters(writers, files)

	kv := initBadger()
	defer kv.Close()

	ioWG.Add(1)
	go writeToCSVs(writers, ioJobs, &ioWG)

	exportNameStrings(kv, ioJobs, &nameStringsWG)
	prepareIndexData(kv)
	nameStringsWG.Wait()

	exportNameStringIndices(kv, ioJobs, &indexWG)
	indexWG.Wait()

	exportVernaculars(ioJobs)

	close(ioJobs)
	ioWG.Wait()
}

func exportVernaculars(ioJobs chan<- ioJob) {
	vernacularMap := make(map[string]string)
	f := gniFile("vernacular_strings")
	r := csv.NewReader(f)

	fmt.Println("Export to vernacular_strings")
	records, err := r.ReadAll()
	check(err)

	for _, v := range records {
		vernacularID := v[0]
		vernacularName := v[1]
		vernacularUUID := uuid.NewV5(gnNameSpace, vernacularName).String()
		vernacularMap[vernacularID] = vernacularUUID
		ioJobs <- ioJob{"vernacular", []string{vernacularUUID, vernacularName}}
	}

	fmt.Println("Export to vernacular_string_indices")
	f2 := gniFile("vernacular_string_indices")
	r2 := csv.NewReader(f2)

	records2, err := r2.ReadAll()
	check(err)
	var dataSourceID, taxonID, vernacularStringID, language, locality,
		countryCode string
	for _, v := range records2 {
		unpackSlice(v, &dataSourceID, &taxonID, &vernacularStringID, &language,
			&locality, &countryCode)
		vernacularStringID = vernacularMap[vernacularStringID]
		csvRow := []string{dataSourceID, taxonID, vernacularStringID, language,
			locality, countryCode}
		ioJobs <- ioJob{"vernacular_index", csvRow}
	}
}

func exportNameStringIndices(kv *badger.KV, ioJobs chan<- ioJob,
	indexWG *sync.WaitGroup) {
	indexJobs := make(chan [][]string)

	for i := 1; i <= workersNum(); i++ {
		indexWG.Add(1)
		go indexWorker(i, indexJobs, ioJobs, indexWG, kv)
	}

	go collectIndexJobs(indexJobs)
}

func indexWorker(workerID int, indexJobs <-chan [][]string, ioJobs chan<- ioJob,
	indexWG *sync.WaitGroup, kv *badger.KV) {
	defer indexWG.Done()
	for {
		job, more := <-indexJobs
		if more {
			log.Printf("NSIndex export %d: %s", workerID, job[0][0:2])
			exportIndexRows(job, ioJobs, kv)
		} else {
			return
		}
	}
}

func exportIndexRows(job [][]string, ioJobs chan<- ioJob, kv *badger.KV) {
	for _, row := range job {
		indexRowToIO(row, ioJobs, kv)
	}
}

func indexRowToIO(row []string, ioJobs chan<- ioJob, kv *badger.KV) {
	var dataSourceID, nameStringID, url, taxonID, globalID, localID,
		nomenclaturalCodeID, rank, acceptedTaxonID, classificationPath,
		classificationPathIDs, classificationPathRanks string

	unpackSlice(row, &dataSourceID, &nameStringID, &url, &taxonID, &globalID,
		&localID, &nomenclaturalCodeID, &rank, &acceptedTaxonID,
		&classificationPath, &classificationPathIDs, &classificationPathRanks)

	pn := parsedNameFromID(nameStringID, kv)

	acceptedNameUUID, acceptedName := findAcceptedName(dataSourceID,
		acceptedTaxonID, kv)

	if acceptedNameUUID == "" {
		acceptedTaxonID = ""
	}

	csvRow := []string{dataSourceID, pn.ID, url, taxonID, globalID,
		localID, nomenclaturalCodeID, rank, acceptedTaxonID, classificationPath,
		classificationPathRanks, classificationPathIDs, acceptedNameUUID,
		acceptedName}
	ioJobs <- ioJob{"index", csvRow}
}

func findAcceptedName(dataSourceID string, taxonID string,
	kv *badger.KV) (string, string) {
	var item badger.KVItem
	var acceptedName, acceptedNameUUID string
	key := indexKey(dataSourceID, taxonID)

	err := kv.Get(key, &item)
	check(err)

	res := item.Value()

	if res == nil {
		acceptedName, acceptedNameUUID = "", ""
	} else {
		parsedName := parsedNameFromID(string(res), kv)
		acceptedName = parsedName.Name
		acceptedNameUUID = parsedName.ID
	}
	return acceptedNameUUID, acceptedName
}

func unpackSlice(row []string, vars ...*string) {
	for i, str := range row {
		*vars[i] = str
	}
}

func collectIndexJobs(indexJobs chan<- [][]string) {
	log.Println("Export name_string_indices to CSV file")
	f := gniFile("name_string_indices")
	chunkSize := 10000
	r := csv.NewReader(f)

	//skip header
	_, err := r.Read()
	check(err)

	i := 0
	rows := make([][]string, chunkSize)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		check(err)
		if i < chunkSize {
			rows[i] = row
		} else {
			indexJobs <- rows
			i = 0
			rows = make([][]string, chunkSize)
			rows[i] = row
		}
		i++
	}
	indexJobs <- rows

	close(indexJobs)
}

func exportNameStrings(kv *badger.KV, ioJobs chan<- ioJob,
	nameStringsWG *sync.WaitGroup) {
	nameStringsJobs := make(chan [][]string)

	for i := 1; i <= workersNum(); i++ {
		nameStringsWG.Add(1)
		go nameStringsWorker(i, nameStringsJobs, ioJobs, nameStringsWG, kv)
	}

	go collectNameStringsJobs(nameStringsJobs)
}

func prepareIndexData(kv *badger.KV) {
	log.Println("Getting name_string_indices from CSV file")
	f := gniFile("name_string_indices")
	r := csv.NewReader(f)

	//skip header
	_, err := r.Read()
	check(err)

	i := 0
	count := 0
	rows := make([][]string, 10000)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		check(err)
		if i < 10000 {
			rows[i] = row
		} else {
			count += i
			if count%100000 == 0 {
				log.Printf("Saved %d index keys\n", count)
			}
			storeIndexData(rows, kv)
			i = 0
			rows[i] = row
		}
		i++
	}
	// Some of the duplicates are writen second time, but it is a drop in a
	// bucket. It is OK to send slices by value, as only header will be
	// copied, the slice itself is send in the header by reference
	storeIndexData(rows, kv)
}

func indexKey(dataSourceID string, taxonID string) []byte {
	key0 := append([]byte(dataSourceID), byte('|'))
	return append(key0, []byte(taxonID)...)
}

func storeIndexData(rows [][]string, kv *badger.KV) {
	entries := badgerizeIndexes(rows)
	err := kv.BatchSet(entries)
	check(err)
}

func badgerizeIndexes(rows [][]string) []*badger.Entry {
	batchSize := len(rows)
	entries := make([]*badger.Entry, batchSize)
	for i, row := range rows {
		key := indexKey(row[0], row[3])
		value := []byte(row[1])
		entry := badger.Entry{Key: key, Value: value}
		entries[i] = &entry
	}
	return entries
}

func writeToCSVs(writers map[string]*csv.Writer, ioJobs <-chan ioJob,
	ioWG *sync.WaitGroup) {
	defer ioWG.Done()
	log.Println("Waiting for ioJobs")
	for job := range ioJobs {
		err := writers[job.Writer].Write(job.Row)
		check(err)
	}
}

func collectNameStringsJobs(nameStringsJobs chan<- [][]string) {
	gniRecords := readCSVNameStrings()
	totalSize := len(gniRecords)
	chunkSize := 10000

	for i := 1; i < totalSize; i += chunkSize {
		end := i + chunkSize
		if end > totalSize {
			end = totalSize
		}
		nameStringsJobs <- gniRecords[i:end]
	}
	close(nameStringsJobs)
}

func nameStringsWorker(workerID int, nameStringsJobs <-chan [][]string,
	ioJobs chan<- ioJob, nameStringsWG *sync.WaitGroup, kv *badger.KV) {
	defer nameStringsWG.Done()
	for {
		job, more := <-nameStringsJobs
		if more {
			log.Printf("NS export %d: %s", workerID, job[0][1])
			processNameStringsRows(job, ioJobs, kv)
		} else {
			return
		}
	}
}

func processNameStringsRows(job [][]string, ioJobs chan<- ioJob,
	kv *badger.KV) {
	for _, row := range job {
		pn := parsedNameFromID(row[0], kv)
		processWords(&pn, ioJobs)
		csvRow := []string{pn.ID, pn.Name, pn.IDCanonical, pn.Canonical,
			strconv.FormatBool(pn.Surrogate)}
		ioJobs <- ioJob{"name_strings", csvRow}
	}
}

func parsedNameFromID(nameStringID string, kv *badger.KV) ParsedName {
	var item badger.KVItem
	err := kv.Get([]byte(nameStringID), &item)
	check(err)
	record := bytes.NewBuffer(item.Value())
	return decodeGob(*record)
}

func processWords(parsedName *ParsedName, ioJobs chan<- ioJob) {
	pos := parsedName.Positions
	id := parsedName.ID
	name := parsedName.Name

	for _, v := range pos {
		wordUpper := strings.ToUpper(name[v.Start:v.End])
		word := strings.Trim(wordUpper, " ")
		switch v.Meaning {
		case "uninomial":
			ioJobs <- ioJob{"uninomial", []string{word, id}}
		case "genus":
			ioJobs <- ioJob{"genus", []string{word, id}}
		case "specific_epithet":
			ioJobs <- ioJob{"species", []string{word, id}}
		case "infraspecific_epithet":
			ioJobs <- ioJob{"subspecies", []string{word, id}}
		case "author_word":
			ioJobs <- ioJob{"author_word", []string{word, id}}
		case "year":
			yr, err := strconv.Atoi(word)
			if err != nil {
				yr = 1
			}
			now := time.Now()
			maxYear := now.Year() + 2
			if (yr >= 1753) && (yr <= maxYear) {
				ioJobs <- ioJob{"year", []string{word, id}}
			}
		}
	}
}

func closeWriters(writers map[string]*csv.Writer, files map[string]*os.File) {
	for name, w := range writers {
		log.Println("Flushing writer", name)
		w.Flush()
	}

	for name, f := range files {
		log.Println("Closing file", name)
		f.Sync()
		f.Close()
	}
}

func initTables() (map[string]*csv.Writer, map[string]*os.File) {
	var files = map[string]*os.File{
		"name_strings":     pgCsvFile("name_strings"),
		"author_word":      pgCsvFile("name_strings__author_words"),
		"genus":            pgCsvFile("name_strings__genus"),
		"species":          pgCsvFile("name_strings__species"),
		"subspecies":       pgCsvFile("name_strings__subspecies"),
		"uninomial":        pgCsvFile("name_strings__uninomial"),
		"year":             pgCsvFile("name_strings__year"),
		"index":            pgCsvFile("name_string_indices"),
		"vernacular":       pgCsvFile("vernacular_strings"),
		"vernacular_index": pgCsvFile("vernacular_string_indices")}

	writers := make(map[string]*csv.Writer)
	for k, v := range files {
		writers[k] = csv.NewWriter(v)
	}

	for k, v := range writers {
		if k == "name_strings" {
			err := v.Write([]string{"id", "name",
				"canonical_uuid", "canonical", "surrogate"})
			check(err)
		} else if k == "index" {
			err := v.Write([]string{"data_source_id", "name_string_id",
				"url,taxon_id", "global_id", "local_id", "nomenclatural_code_id",
				"rank", "accepted_taxon_id", "classification_path",
				"classification_path_ids", "classification_path_ranks",
				"accepted_name_uuid", "accepted_name"})
			check(err)
		} else if k == "vernacular" {
			err := v.Write([]string{"id", "name"})
			check(err)
		} else if k == "vernacular_index" {
			err := v.Write([]string{"data_source_id", "taxon_id",
				"vernacular_string_id", "language", "locality", "country_code"})
			check(err)
		} else {
			err := v.Write([]string{k, "name_uuid"})
			check(err)
		}
	}
	return writers, files
}

func pgCsvFile(f string) *os.File {
	file, err := os.Create("/tmp/gnindex_pg/" + f + ".csv")
	check(err)
	return file
}
