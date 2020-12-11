package util

import (
	"fmt"
	"strconv"
)

func TypeToInt(num interface{}) int {
	if val, ok := num.(int); ok {
		return val
	}

	var intVal int
	if val, ok := num.(float64); ok {
		intVal = int(val)
	}
	if val, ok := num.(string); ok {
		intVal, _ = strconv.Atoi(val)
	}
	return intVal
}

func MapFieldCheck(target map[string]interface{}, keys ...string) error {
	for _, key := range keys {
		if _, ok := target[key]; !ok {
			return fmt.Errorf(key+" is not in %+v\n", target)
		}
	}
	return nil
}
