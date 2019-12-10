package json

import (
	"encoding/json"
	"reflect"

	log "github.com/sirupsen/logrus"
)

type JSON struct {
	Append map[string]interface{}
}

func (j *JSON) Transform(message string) (transformed string, err error) {
	// unmarshal
	var obj map[string]interface{}
	err = json.Unmarshal([]byte(message), &obj)
	if err != nil {
		log.Print(err)
	}

	// do stuff
	for k, v := range j.Append {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Func:
			obj[k] = v.(func() interface{})()
		default:
			obj[k] = v
		}
	}

	// marshal
	newObj, err := json.Marshal(obj)
	transformed = string(newObj)

	return
}

func (j *JSON) Info() {
	log.Info("Using JSON Transformer.")
}
