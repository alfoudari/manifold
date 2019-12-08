package json

import (
	log "github.com/sirupsen/logrus"
	"encoding/json"
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
		obj[k] = v
	}

	// marshal
	newObj, err := json.Marshal(obj)
	transformed = string(newObj)

	return
}