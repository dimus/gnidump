package main

import (
	"bytes"
	"encoding/csv"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger"
	jsoniter "github.com/json-iterator/go"
	uuid "github.com/satori/go.uuid"
)

var gnNameSpace = uuid.NewV5(uuid.NamespaceDNS, "globalnames.org")

func convertTables() {
	parsingJobs := make(chan map[string]string, 100)
	done := make(chan bool)

	resetKV()

	kv := initBadger()
	defer kv.Close()

	for i := 1; i <= workersNum(); i++ {
		go parserWorker(i, parsingJobs, done, kv)
	}

	go prepareJobs(parsingJobs)

	<-done
	// Give time to workers to finish last changes. When `done` channel
	// gets a value from one worker, other workers might still have some
	// some processing going.
	log.Println("Tearing down...")
	time.Sleep(5 * time.Second)
}

func resetKV() {
	log.Println("Cleaning up key value store")
	d, err := os.Open(badgerDir)
	check(err)
	defer d.Close()
	names, err := d.Readdirnames(-1)
	check(err)
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(badgerDir, name))
		check(err)
	}
}

func parserWorker(id int, parsingJobs <-chan map[string]string,
	done chan<- bool, kv *badger.KV) {
	for {
		j, more := <-parsingJobs
		if more {
			parsedNames := parseNamesBatch(j)
			storeParsedNames(&parsedNames, kv)
		} else {
			done <- true
			return
		}
	}
}

func storeParsedNames(parsedNames *[]ParsedName, kv *badger.KV) {
	entries := badgerize(parsedNames)
	kv.BatchSet(entries)
}

func badgerize(parsedNames *[]ParsedName) []*badger.Entry {
	batchSize := len(*parsedNames) * 2
	var entries = make([]*badger.Entry, batchSize)
	for i, v := range *parsedNames {
		encodedParsedName := v.encodeGob()
		e1 := badger.Entry{Key: []byte(v.ID), Value: encodedParsedName.Bytes()}
		e2 := badger.Entry{Key: []byte(v.IDOriginal),
			Value: encodedParsedName.Bytes()}
		index := i * 2
		entries[index] = &e1
		entries[index+1] = &e2
	}
	return entries
}

func parseNamesBatch(namesMap map[string]string) []ParsedName {
	namesArray := prepareArray(namesMap)
	parsedNames := remoteParser(namesArray)
	for i := range parsedNames {
		p := &parsedNames[i]
		p.IDOriginal = namesMap[p.Name]
	}
	log.Printf("Parsed '%s'\n", parsedNames[0].Canonical)
	return parsedNames
}

func nameFromJob(job map[string]string) string {
	var name string
	for k := range job {
		name = k
		break
	}
	return name
}

func prepareJobs(parsingJobs chan<- map[string]string) {
	records := readCSVNameStrings()

	log.Println("Getting names parsed")
	totalSize := len(records)
	chunkSize := 10000
	for i := 1; i < totalSize; i += chunkSize {
		end := i + chunkSize
		if end > totalSize {
			end = totalSize
		}
		parsingJobs <- namesMap(records[i:end])
	}
	close(parsingJobs)
}

func readCSVNameStrings() [][]string {
	log.Println("Getting name_strings from CSV file")
	f := gniFile("name_strings")
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	check(err)
	return records
}

func remoteParser(names []string) []ParsedName {
	namesJSON, err := jsoniter.Marshal(names)
	check(err)
	namesReader := bytes.NewReader(namesJSON)
	env := envVars()
	res, err := http.Post(env["parser_url"], "application/json",
		namesReader)
	check(err)
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	check(err)
	return parseJSON(body)
}

func parseJSON(json []byte) []ParsedName {
	var data map[string]interface{}
	parsed := []ParsedName{}
	err := jsoniter.Unmarshal(json, &data)
	check(err)
	names := data["namesJson"].([]interface{})

	for i := range names {
		name := names[i].(map[string]interface{})
		parsedName := createParsedName(name)
		parsed = append(parsed, parsedName)
	}
	return parsed
}

func createParsedName(name map[string]interface{}) ParsedName {
	id := name["name_string_id"].(string)
	verbatim := name["verbatim"].(string)
	parsed := name["parsed"].(bool)
	idCanonical, idOriginal, canonical := "", "", ""
	surrogate := false
	positions := []Position{}
	if parsed {
		canonicalMap := name["canonical_name"].(map[string]interface{})
		canonical = canonicalMap["value"].(string)
		idCanonical = uuid.NewV5(gnNameSpace, canonical).String()
		surrogate = name["surrogate"].(bool)
		positions = createPositions(name["positions"].([]interface{}))
	}
	return ParsedName{id, idCanonical, idOriginal, verbatim, canonical,
		surrogate, positions}
}

func createPositions(pos []interface{}) []Position {
	var positions []Position
	for i := range pos {
		posAry := pos[i].([]interface{})
		wordType := posAry[0].(string)
		start := posAry[1].(float64)
		end := posAry[2].(float64)
		positions = append(positions, Position{wordType, int(start), int(end)})
	}
	return positions
}

func prepareArray(m map[string]string) []string {
	names := make([]string, len(m))
	i := 0
	for n := range m {
		names[i] = n
		i++
	}
	return names
}

func namesMap(records [][]string) map[string]string {
	res := make(map[string]string)
	for _, record := range records {
		res[record[1]] = record[0]
	}
	return res
}

func gniFile(f string) *os.File {
	file, err := os.Open("/tmp/gni_mysql/" + f + ".csv")
	check(err)
	return file
}
