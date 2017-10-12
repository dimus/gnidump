// Package parser helps to convert JSON respresentation of gnparser output
// into util.ParsedName structures.
package parser

import (
	"github.com/dimus/gnidump/util"
	jsoniter "github.com/json-iterator/go"
)

// Output represents root of JSON payload.
type Output struct {
	Results []ParsedJSON `json:"namesJson"`
}

// ParsedJSON collects conversion from JSON to an intermediary structure.
type ParsedJSON struct {
	ID        string         `json:"name_string_id"`
	Name      string         `json:"verbatim"`
	Parsed    bool           `json:"parsed"`
	Surrogate bool           `json:"surrogate"`
	Canonical Canonical      `json:"canonical_name"`
	Positions []PositionJSON `json:"positions"`
}

// Canonical contains canonical names
type Canonical struct {
	Canonical         string `json:"value"`
	CanonicalWithRank string `json:"value_ranked"`
}

// PositionJSON is an array representation of Position data from JSON.
type PositionJSON [3]interface{}

// Meaning returns semantic meaning of a word.
func (p *PositionJSON) Meaning() string {
	wt := p[0].(string)
	return wt
}

// Start returns first letter of a word.
func (p *PositionJSON) Start() int {
	return int(p[1].(float64))
}

// End returns last letter of a word.
func (p *PositionJSON) End() int {
	return int(p[2].(float64))
}

// ParsedNamesFromJSON returns a slice of util.ParsedName structures.
func ParsedNamesFromJSON(parserOutput []byte) []util.ParsedName {
	var res Output
	err := jsoniter.Unmarshal(parserOutput, &res)
	util.Check(err)
	return parsedNames(res.Results)
}

func parsedNames(p []ParsedJSON) []util.ParsedName {
	parsedNames := make([]util.ParsedName, len(p))
	for i, v := range p {
		parsedNames[i] = parsedName(v)
	}
	return parsedNames
}

func parsedName(j ParsedJSON) util.ParsedName {
	pn := util.ParsedName{ID: j.ID, Name: j.Name}
	if j.Parsed {
		pn.IDCanonical = util.ToUUID(j.Canonical.Canonical)
		pn.Canonical = j.Canonical.Canonical
		if j.Canonical.Canonical != j.Canonical.CanonicalWithRank {
			pn.CanonicalWithRank = j.Canonical.CanonicalWithRank
		}
		pn.Surrogate = j.Surrogate
		pn.Positions = positions(j.Positions)
	}
	return pn
}

func positions(j []PositionJSON) []util.Position {
	pos := make([]util.Position, len(j))
	for i, v := range j {
		pos[i] = util.Position{v.Meaning(), v.Start(), v.End()}
	}
	return pos
}
