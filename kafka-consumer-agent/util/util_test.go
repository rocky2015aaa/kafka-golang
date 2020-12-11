package util

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTypeToInt(t *testing.T) {
	fmt.Println("--- TestTypeToInt Success Case Test ---")

	var successTests = []struct {
		num      interface{}
		expected int
	}{
		{1, 1},
		{"2", 2},
		{float64(3.0), 3},
	}

	for _, test := range successTests {
		intVal := TypeToInt(test.num)
		if !reflect.DeepEqual(test.expected, intVal) {
			t.Errorf("TypeToInt(%v) was incorrect, got: %v want: %v", test.num, intVal, test.expected)
		}
	}
}
