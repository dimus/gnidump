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

var githash = "n/a"
var buildstamp = "n/a"

func main() {
	command := ""
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	dump.Prepare()
	switch command {
	case "version":
		fmt.Printf(" Git commit hash: %s\n UTC Build Time: %s\n\n",
			githash, buildstamp)
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
	gnidump create
`
		fmt.Println(help)
	}
}
