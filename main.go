// Application `gnidump` takes data from MySQL database of `gni`, and converts
// the data into format required by `gnindex`.
package main

import (
	"fmt"
	"os"

	"github.com/dimus/gnidump/converter"
	"github.com/dimus/gnidump/creator"
	"github.com/dimus/gnidump/dump"
)

func main() {
	command := ""
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	dump.Prepare()
	switch command {
	case "dump":
		dump.Tables()
	case "convert":
		converter.Data()
	case "create":
		creator.Tables()
	default:
		help := `
Usage:
  gnidump dump
  gnidump convert
`
		fmt.Println(help)
	}
}
