package util

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"strconv"

	badger "github.com/dgraph-io/badger"
)

// BudgerDir is a direcotry to the badger key-value store.
const BadgerDir string = "/tmp/badger"

// Position describes semantic meaning of a word that appears between
// the start and end positions.
type Position struct {
	Meaning string
	Start   int
	End     int
}

// ParsedName is a collection of all necessary information from the
// scientific name parser.
type ParsedName struct {
	ID          string
	IDCanonical string
	IDOriginal  string
	Name        string
	Canonical   string
	Surrogate   bool
	Positions   []Position
}

// ParsedName.EncodeGob is a method for serlializing ParsedName value.
func (pn ParsedName) EncodeGob() bytes.Buffer {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(pn)
	Check(err)
	return b
}

// DecodeGob deserializes bytes buffer to ParsedName struct.
func DecodeGob(b bytes.Buffer) ParsedName {
	var pn ParsedName
	dec := gob.NewDecoder(&b)
	err := dec.Decode(&pn)
	Check(err)
	return pn
}

// Returns number of workers by reading it from WORKERS_NUMBER environment
// variable.
func WorkersNum() int {
	env := EnvVars()

	workersNum, err := strconv.Atoi(env["workers"])
	Check(err)
	return workersNum
}

// Check(err) is a simple placeholder for error handling.
func Check(err error) {
	if err != nil {
		panic(err)
	}
}

// InitBadger finds and initializes connection to a badger key-value store.
// If the store does not exist, InitBadger creates it.
func InitBadger() *badger.KV {
	log.Println("Starting key value store")
	opts := badger.DefaultOptions
	opts.Dir = BadgerDir
	opts.ValueDir = BadgerDir
	kv, err := badger.NewKV(&opts)
	Check(err)
	return kv
}

// EnvVars imports all environment variables relevant for the data conversion.
func EnvVars() map[string]string {
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