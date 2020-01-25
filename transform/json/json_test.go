package json

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransform(t *testing.T) {
	transformer := &JSON{
		Append: map[string]interface{}{
			"key": 1.5,
		},
	}

	json := `{"a":1, "b":"2", "c":true, "d":null}`
	json_transformed := `{"a":1, "b":"2", "c":true, "d":null, "key":1.5}`

	transformed, err := transformer.Transform(json)
	if err != nil {
		t.Fatal(err)
	}

	assert.JSONEq(t, transformed, json_transformed)
}