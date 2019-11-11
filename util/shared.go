package util

import (
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"path/filepath"
	"strconv"

	badger "github.com/dgraph-io/badger"
	"gitlab.com/gogna/gnparser/pb"
)

// BudgerDir is a direcotry to the badger key-value store.
const (
	BadgerDir  = "/opt/gnidump/badger/"
	GniDir     = "/opt/gnidump/gni_mysql/"
	GnindexDir = "/opt/gnidump/gnindex_pg/"
)

// ParsedName is a collection of all necessary information from the
// scientific name parser.
type ParsedName struct {
	ID                string
	IDCanonical       string
	IDOriginal        string
	Name              string
	Canonical         string
	CanonicalWithRank string
	Surrogate         bool
	Positions         []*pb.Position
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
func InitBadger() *badger.DB {
	log.Println("Starting key value store")
	bdb, err := badger.Open(badger.DefaultOptions(BadgerDir))
	Check(err)
	return bdb
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

// CleanDir removes all files from a directory.
func CleanDir(dir string) {
	d, err := os.Open(dir)
	Check(err)
	defer d.Close()

	names, err := d.Readdirnames(-1)
	Check(err)
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		Check(err)
	}
}
