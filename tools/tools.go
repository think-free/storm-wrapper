package tools

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

// FillStruct can be use to fill a structure from a map[string] inteface{}
func FillStruct(m map[string]interface{}, s interface{}) error {

	structValue := reflect.ValueOf(s).Elem()

	for name, value := range m {
		structFieldValue := structValue.FieldByName(name)

		if !structFieldValue.IsValid() {
			return fmt.Errorf("No such field: %s in obj", name)
		}

		if !structFieldValue.CanSet() {
			return fmt.Errorf("Cannot set %s field value", name)
		}

		val := reflect.ValueOf(value)
		if structFieldValue.Type() != val.Type() {
			return errors.New("Provided value type didn't match obj field type")
		}

		structFieldValue.Set(val)
	}
	return nil
}

