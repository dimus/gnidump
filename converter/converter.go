// Package converter parses name-strings from gni-generated CSV files
// and stores this information into	`badger` key-value store
package converter

import (
	"bytes"
	"encoding/csv"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"

	badger "github.com/dgraph-io/badger"
	"github.com/dimus/gnidump/parser"
	"github.com/dimus/gnidump/util"
	jsoniter "github.com/json-iterator/go"
)

// Data fetches data needed for gnindex and stores it in a key-value store.
func Data() {
	parsingJobs := make(chan map[string]string, 100)
	var wg sync.WaitGroup

	resetKV()

	kv := util.InitBadger()
	defer kv.Close()

	for i := 1; i <= util.WorkersNum(); i++ {
		wg.Add(1)
		go parserWorker(i, parsingJobs, &wg, kv)
	}

	go prepareJobs(parsingJobs)

	wg.Wait()
}

// ReadCSVNameStrings reads all lines from gni's name_strings.csv into memory.
func ReadCSVNameStrings() [][]string {
	log.Println("Getting name_strings from CSV file")
	f := GniFile("name_strings")
	r := csv.NewReader(f)
	records, err := r.ReadAll()
	util.Check(err)
	return records
}

// GniFile returns handles to existing CSV files with gni dumps.
func GniFile(f string) *os.File {
	file, err := os.Open(util.GniDir + f + ".csv")
	util.Check(err)
	return file
}

func resetKV() {
	log.Println("Cleaning up key value store")
	util.CleanDir(util.BadgerDir)
}

func parserWorker(id int, parsingJobs <-chan map[string]string,
	wg *sync.WaitGroup, kv *badger.KV) {
	defer wg.Done()
	for {
		j, more := <-parsingJobs
		if more {
			parsedNames := parseNamesBatch(j)
			storeParsedNames(&parsedNames, kv)
		} else {
			return
		}
	}
}

func storeParsedNames(parsedNames *[]util.ParsedName, kv *badger.KV) {
	entries := badgerize(parsedNames)
	err := kv.BatchSet(entries)
	util.Check(err)
}

func badgerize(parsedNames *[]util.ParsedName) []*badger.Entry {
	batchSize := len(*parsedNames) * 2
	var entries = make([]*badger.Entry, batchSize)
	for i, v := range *parsedNames {
		encodedParsedName := v.EncodeGob()
		e1 := badger.Entry{Key: []byte(v.ID), Value: encodedParsedName.Bytes()}
		e2 := badger.Entry{Key: []byte(v.IDOriginal),
			Value: encodedParsedName.Bytes()}
		index := i * 2
		entries[index] = &e1
		entries[index+1] = &e2
	}
	return entries
}

func parseNamesBatch(namesMap map[string]string) []util.ParsedName {
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
	records := ReadCSVNameStrings()

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

func remoteParser(names []string) []util.ParsedName {
	namesJSON, err := jsoniter.Marshal(names)
	util.Check(err)
	namesReader := bytes.NewReader(namesJSON)
	env := util.EnvVars()
	res, err := http.Post(env["parser_url"], "application/json",
		namesReader)
	util.Check(err)
	body, err := ioutil.ReadAll(res.Body)
	err = res.Body.Close()
	util.Check(err)
	util.Check(err)
	return parser.ParsedNamesFromJSON(body)
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
