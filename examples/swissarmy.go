package examples

import (
	"os"
)

// get env variable value or return default
func GetEnv(key string, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}