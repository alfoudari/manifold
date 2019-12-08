package json

import (
	"fmt"
	"testing"
)

func TestTransform(t *testing.T) {
	transformer := &JSON{
		Append: map[string]interface{}{
			"key": 1.5,
		},
	}

	json := `{"a":1, "b":"2", "c":true, "d":null}`

	transformed, err := transformer.Transform(json)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(transformed)
}