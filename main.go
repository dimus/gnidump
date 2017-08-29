package main

import (
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	command := ""
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	prepare()
	switch command {
	case "dump":
		dumpTables()
	case "convert":
		convertTables()
	case "create":
		createTables()
	default:
		help := `
Usage:
  gnidump dump
  gnidump convert
`
		fmt.Println(help)
	}
}
