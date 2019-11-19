// Package converter parses name-strings from gni-generated CSV files
// and stores this information into	`badger` key-value store
package converter

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger"
	"github.com/dimus/gnidump/util"
	"github.com/gnames/uuid5"
	"gitlab.com/gogna/gnparser"
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
	wg *sync.WaitGroup, kv *badger.DB) {
	gnp := gnparser.NewGNparser()
	defer wg.Done()
	for {
		j, more := <-parsingJobs
		if more {
			parsedNames := parseNamesBatch(gnp, j)
			storeParsedNames(&parsedNames, kv)
		} else {
			return
		}
	}
}

func storeParsedNames(parsedNames *[]util.ParsedName, kv *badger.DB) {
	var err error
	entries := badgerize(parsedNames)
	wb := kv.NewWriteBatch()
	for _, v := range entries {
		err = wb.SetEntry(v)
		if err != nil {
			log.Fatal(err)
		}
	}
	err = wb.Flush()
	if err != nil {
		log.Fatal(err)
	}
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

func parseNamesBatch(gnp gnparser.GNparser,
	namesMap map[string]string) []util.ParsedName {
	parsedNames := make([]util.ParsedName, len(namesMap))
	count := 0
	for name, id := range namesMap {
		parsed := parseName(gnp, name, id)
		parsedNames[count] = parsed
		count++
	}
	log.Printf("Parsed '%s'\n", parsedNames[0].Canonical)
	return parsedNames
}

func parseName(gnp gnparser.GNparser, name, origID string) util.ParsedName {
	p := gnp.ParseToObject(name)
	var canonical, canonicalWithRank, idCanonical string
	if p.Canonical != nil {
		canonical = p.Canonical.Simple
		canonicalWithRank = p.Canonical.Full
		idCanonical = uuid5.UUID5(p.Canonical.Simple).String()
	}
	fmt.Println(p.NameType)
	return util.ParsedName{
		ID:                p.Id,
		IDCanonical:       idCanonical,
		IDOriginal:        origID,
		Name:              name,
		Canonical:         canonical,
		CanonicalWithRank: canonicalWithRank,
		Surrogate:         isSurrogate(p.NameType.String()),
		Positions:         p.Positions,
	}
}

func isSurrogate(s string) bool {
	fmt.Println(s)
	return strings.HasSuffix(s, "SURROGATE")
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

func namesMap(records [][]string) map[string]string {
	res := make(map[string]string)
	for _, record := range records {
		res[record[1]] = record[0]
	}
	return res
}
