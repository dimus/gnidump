package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"strconv"

	badger "github.com/dgraph-io/badger"
)

const badgerDir string = "/tmp/badger"

// Position describes semantic meaning of a word that appears between
// the start and end positions
type Position struct {
	Meaning string
	Start   int
	End     int
}

// ParsedName is a collection of all necessary information from the
// scientific name parser
type ParsedName struct {
	ID          string
	IDCanonical string
	IDOriginal  string
	Name        string
	Canonical   string
	Surrogate   bool
	Positions   []Position
}

func (pn ParsedName) encodeGob() bytes.Buffer {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(pn)
	check(err)
	return b
}

func decodeGob(b bytes.Buffer) ParsedName {
	var pn ParsedName
	dec := gob.NewDecoder(&b)
	err := dec.Decode(&pn)
	check(err)
	return pn
}

func workersNum() int {
	env := envVars()

	workersNum, err := strconv.Atoi(env["workers"])
	check(err)
	return workersNum
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func initBadger() *badger.KV {
	log.Println("Starting key value store")
	opts := badger.DefaultOptions
	opts.Dir = badgerDir
	opts.ValueDir = badgerDir
	kv, err := badger.NewKV(&opts)
	check(err)
	return kv
}
