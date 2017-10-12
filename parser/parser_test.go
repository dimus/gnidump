package parser

import (
	"reflect"
	"testing"

	"github.com/dimus/gnidump/util"
)

func TestParseJson(t *testing.T) {
	noParse := []byte(`{"namesJson":[{"parsed":false,"verbatim":"noparse",
		"surrogate":false,"parser_version":"0.4.1", "virus":false,
		"name_string_id":"7fffcdf1-2162-5aa3-a506-0b844283de22",
		"bacteria":false}]}`)

	canonical := []byte(`{"namesJson":[
	  {"quality":1,"parsed":true,"verbatim":"Homo sapiens",
		"surrogate":false,"parser_version":"0.4.1",
		"normalized":"Homo sapiens","virus":false,
		"positions":[["genus",0,4],["specific_epithet",5,12]],
		"name_string_id":"16f235a0-e4a3-529c-9b83-bd15fe722110",
		"canonical_name":{"value":"Homo sapiens", "value_ranked":"Homo sapiens"},
		"hybrid":false, "details":[{"genus":{"value":"Homo"},
		"specific_epithet":{"value":"sapiens"}}],"bacteria":false}]}`)

	rankedCanonical := []byte(`{"namesJson":
	  [{"quality":1,"parsed":true,
		"verbatim":"Acacia bidentata Benth. var. pubescens Meisn.",
		"surrogate":false,"parser_version":"0.4.1",
		"normalized":"Acacia bidentata Benth. var. pubescens Meisn.",
		"virus":false,"positions":[["genus",0,6],["specific_epithet",7,16],
		["author_word",17,23],["rank",24,28],["infraspecific_epithet",29,38],
		["author_word",39,45]],
		"name_string_id":"ada76998-a975-59a8-b809-46778c876ab2",
		"canonical_name":{"value":"Acacia bidentata pubescens",
		"value_ranked":"Acacia bidentata var. pubescens"},"hybrid":false,
		"details":[{"genus":{"value":"Acacia"},
		"specific_epithet":{"value":"bidentata",
		"authorship":{"value":"Benth.",
		"basionym_authorship":{"authors":["Benth."]}}},
		"infraspecific_epithets":[{"value":"pubescens","rank":"var.",
		"authorship":{"value":"Meisn.",
		"basionym_authorship":{"authors":["Meisn."]}}}]}],"bacteria":false}]}`)

	noParseRes := ParsedNamesFromJSON(noParse)
	noParseExpect := util.ParsedName{ID: "7fffcdf1-2162-5aa3-a506-0b844283de22",
		Name: "noparse", Surrogate: false}

	if !reflect.DeepEqual(noParseRes[0], noParseExpect) {
		t.Error("Wrong result for ", noParseRes)
	}

	canonicalRes := ParsedNamesFromJSON(canonical)
	canonicalExpect := util.ParsedName{
		ID:          "16f235a0-e4a3-529c-9b83-bd15fe722110",
		IDCanonical: "16f235a0-e4a3-529c-9b83-bd15fe722110",
		Name:        "Homo sapiens", Canonical: "Homo sapiens", Surrogate: false,
		Positions: []util.Position{{"genus", 0, 4}, {"specific_epithet", 5, 12}}}
	if !reflect.DeepEqual(canonicalRes[0], canonicalExpect) {
		t.Error("Wrong result for ", canonicalRes)
	}

	rankedCanonicalRes := ParsedNamesFromJSON(rankedCanonical)
	rankedCanonicalExpect := util.ParsedName{
		"ada76998-a975-59a8-b809-46778c876ab2",
		"076fb804-631d-505a-b8ef-332fba9f0c43",
		"", "Acacia bidentata Benth. var. pubescens Meisn.",
		"Acacia bidentata pubescens", "Acacia bidentata var. pubescens", false,
		[]util.Position{{"genus", 0, 6}, {"specific_epithet", 7, 16},
			{"author_word", 17, 23}, {"rank", 24, 28},
			{"infraspecific_epithet", 29, 38}, {"author_word", 39, 45}}}
	if !reflect.DeepEqual(rankedCanonicalRes[0], rankedCanonicalExpect) {
		t.Error("Wrong result for ", rankedCanonicalRes[0])
	}
}
