package gocassa

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"

	r "github.com/monzo/gocassa/reflect"
)

type scanner struct {
	stmt statement

	result   interface{}
	rowCount int
}

func newScanner(stmt statement, result interface{}) *scanner {
	return &scanner{
		stmt:     stmt,
		result:   result,
		rowCount: 0,
	}
}

func getType(in interface{}) reflect.Type {
	elem := reflect.TypeOf(in)
	for elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return elem
}

func getSliceValueType(in interface{}) reflect.Type {
	elem := getType(in).Elem()
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return elem
}

func (s *scanner) ScanAll(iter Scannable) (int, error) {
	switch getType(s.result).Kind() { // TODO: optimise this
	case reflect.Slice:
		return s.iterSlice(iter)
	case reflect.Struct:
		// We are reading a single element here, decode a single row
		return s.iterSingle(iter)
	}

	return 0, fmt.Errorf("can only decode into a struct or slice of structs, not %T", s.result)
}

func (s *scanner) iterSlice(iter Scannable) (int, error) {
	// Extract the type of the slice
	// TODO(suhail): Name these better!
	sliceType := getType(s.result)
	sliceElemType := getType(sliceType.Elem())
	sliceElemValType := getSliceValueType(s.result)

	// To preserve prior bebaviour, if the result slice is not empty
	// then allocate a new slice and set it as the value
	slice := reflect.ValueOf(s.result).Elem()
	if slice.Len() != 0 {
		slice.Set(reflect.Zero(sliceType))
	}

	ptr := reflect.New(sliceElemValType).Interface()
	m, ok := r.StructFieldMap(ptr, true)
	if !ok {
		return 0, fmt.Errorf("could not decode struct of type %T", ptr)
	}

	structFields := s.structFields(m)
	generatePtrs := func() []interface{} {
		ptrs := []interface{}{}
		for _, sf := range structFields {
			if sf != nil {
				val := reflect.New(sf.Type())
				ptrs = append(ptrs, val.Interface())
			} else {
				ptrs = append(ptrs, &ignoreFieldType{})
			}
		}
		return ptrs
	}

	ptrs := generatePtrs()

	for iter.Scan(ptrs...) {
		outPtr := reflect.New(sliceElemValType)
		outVal := outPtr.Elem()

		for index, field := range structFields {
			if field == nil {
				continue
			}

			outField := outVal.Field(field.Index()[0])
			if outField.CanSet() {
				outField.Set(reflect.ValueOf(ptrs[index]).Elem())
			}
		}

		if sliceElemType.Kind() == reflect.Ptr {
			slice.Set(reflect.Append(slice, outPtr))
		} else {
			slice.Set(reflect.Append(slice, outVal))
		}

		ptrs = generatePtrs()
		s.rowCount++
	}
	return s.rowCount, nil
}

func (s *scanner) iterSingle(iter Scannable) (int, error) {
	m, ok := r.StructFieldMap(s.result, true)
	if !ok {
		return 0, fmt.Errorf("could not decode struct of type %T", s.result)
	}

	structFields := s.structFields(m)
	generatePtrs := func() []interface{} {
		ptrs := []interface{}{}
		for _, sf := range structFields {
			if sf != nil {
				val := reflect.New(sf.Type())
				ptrs = append(ptrs, val.Interface())
			} else {
				ptrs = append(ptrs, &ignoreFieldType{})
			}
		}
		return ptrs
	}

	ptrs := generatePtrs()
	scanOk := iter.Scan(ptrs...) // we only need to scan once
	if !scanOk {
		return 0, RowNotFoundError{}
	}

	outPtr := reflect.ValueOf(s.result)
	out := outPtr.Elem()
	for index, field := range structFields {
		if field == nil {
			continue
		}

		outField := out.Field(field.Index()[0])
		if outField.CanSet() {
			outField.Set(reflect.ValueOf(ptrs[index]).Elem())
		}
	}

	s.rowCount++
	return s.rowCount, nil
}

func (s *scanner) structFields(m map[string]r.Field) []*r.Field {
	structFields := []*r.Field{}
	for _, fieldName := range s.stmt.fieldNames {
		field, ok := m[strings.ToLower(fieldName)]
		if !ok { // the field doesn't have a destination
			structFields = append(structFields, nil)
		} else {
			structFields = append(structFields, &field)
		}
	}
	return structFields
}

// This struct is for fields we want to ignore, we specify a custom unmarshal
// type which literally is a no-op and does nothing with this data. In the
// future, maybe we can be smarter of only extracting fields which we are
// able to unmarshal into our target struct
type ignoreFieldType struct{}

func (i *ignoreFieldType) UnmarshalCQL(_ gocql.TypeInfo, _ []byte) error {
	return nil
}
