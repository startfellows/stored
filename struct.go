package stored

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Struct used for work with input structure
type Struct struct {
	editable bool
	value    reflect.Value
}

// setField sets field value using bytes
func (s *Struct) setField(field *Field, data []byte) {
	objField := s.value.Field(field.Num)
	if objField.Kind() == reflect.Ptr {
		if objField.IsNil() {
			// This code is working in main case
			t := field.Value.Type().Elem()
			value := reflect.New(t)
			objField.Set(value) // creating empty object to fill it below

			if len(data) == 0 { // no reason to go further id no data passed
				return
			}
		}
	}

	t := field.Value.Type()
	objValue := reflect.New(t)

	err := msgpack.Unmarshal(data, objValue.Interface())
	if err != nil {
		fmt.Println("Decode to value failed", field.Name, field.object.name, len(data), err)
	}

	objField.Set(reflect.Indirect(objValue))
}

// incField increment field value using interface value
func (s *Struct) incField(field *Field, toInc interface{}) {
	objField := s.value.Field(field.Num)
	inter := objField.Interface()
	switch field.Kind {
	case reflect.Int:
		s.setFieldValue(objField, inter.(int)+toInc.(int))
	case reflect.Uint:
		s.setFieldValue(objField, inter.(uint)+toInc.(uint))
	case reflect.Int32:
		s.setFieldValue(objField, inter.(int32)+toInc.(int32))
	case reflect.Uint32:
		s.setFieldValue(objField, inter.(uint32)+toInc.(uint32))
	case reflect.Int64:
		s.setFieldValue(objField, inter.(int64)+toInc.(int64))
	case reflect.Uint64:
		s.setFieldValue(objField, inter.(uint64)+toInc.(uint64))
	default:
		panic("field " + field.Name + " is not incrementable")
	}
}

// checkNilPtrField check if objField is Nil reflect.Ptr
// If yes, initializes underlying object and return it
func (s *Struct) checkNilPtrField(objField reflect.Value) reflect.Value {
	if objField.Kind() == reflect.Ptr {
		if objField.IsNil() {
			t := objField.Type().Elem()
			newValue := reflect.New(t)

			objField.Set(newValue)
		}

		return reflect.Indirect(objField)
	}

	return objField
}

func (s *Struct) setSliceFieldValue(objField reflect.Value, value interface{}) {
	valueSlice, ok := value.([]interface{})
	if !ok {
		fmt.Println("Filling slice with value slice failed: value slice empty or non existing")
		return
	}

	newSlice := reflect.MakeSlice(objField.Type(), 0, len(valueSlice))
	for _, i := range valueSlice {
		newSlice = reflect.Append(newSlice, reflect.ValueOf(i))
	}

	objField.Set(newSlice)
}

func (s *Struct) setStructFieldValue(objStruct reflect.Value, value interface{}) {
	valueMap, ok := value.(map[string]interface{})
	if !ok {
		fmt.Println("Filling struct with value map failed: value map empty or non existing")
		return
	}

	objStruct = s.checkNilPtrField(objStruct)

	for fieldName, fieldData := range valueMap {
		field := objStruct.FieldByName(fieldName)
		if reflect.ValueOf(field).IsZero() {
			continue
		}

		fieldKind := field.Kind()
		if fieldKind == reflect.Ptr {
			fieldKind = field.Type().Elem().Kind()
		}

		switch fieldKind {
		case reflect.Slice:
			s.setSliceFieldValue(field, fieldData)
		case reflect.Struct:
			s.setStructFieldValue(field, fieldData)
		default:
			s.setFieldValue(field, fieldData)
		}
	}
}

func (s *Struct) setFieldValue(objField reflect.Value, value interface{}) {
	objField = s.checkNilPtrField(objField)

	objField.Set(reflect.ValueOf(value))
}

// Fill will use data inside value object to fill struct
func (s *Struct) Fill(o *Object, v *Value) {
	if !s.editable {
		panic("attempt to change readonly struct")
	}

	for fieldName, binaryValue := range v.raw {
		field, ok := o.mutableFields[fieldName]
		if ok {
			//if len(binaryValue) > 0 { // so raw should be empty in a first place
			s.setField(field, binaryValue)
			//}
		} else {
			//o.log("unknown field «" + fieldName + "», skipping")
			//nothing to worry about
		}
	}
	// decoded used to avoid unnecessary decode and encode
	for fieldName, interfaceValue := range v.decoded {
		field, ok := o.mutableFields[fieldName]
		if ok {
			field.setTupleValue(s.value, interfaceValue)
		}
	}

	immutablesValue, ok := v.raw["*"]
	if ok {
		combinedFields := map[string]interface{}{}
		err := msgpack.Unmarshal(immutablesValue, &combinedFields)
		if err != nil {
			fmt.Println("Fill failed:", err)
			return
		}

		// This is ugly, I have truly recursive solution in mind, but it will work for now.
		for fieldName, fieldData := range combinedFields {
			field, fok := o.immutableFields[fieldName]
			if fok {
				fieldValue := s.value.Field(field.Num)
				fieldKind := fieldValue.Kind()
				if fieldKind == reflect.Ptr {
					fieldKind = fieldValue.Type().Elem().Kind()
				}

				switch fieldKind {
				case reflect.Slice:
					s.setSliceFieldValue(fieldValue, fieldData)
				case reflect.Struct:
					s.setStructFieldValue(fieldValue, fieldData)
				default:
					s.setFieldValue(fieldValue, fieldData)
				}
			}
		}
	}
}

// Get return field as interface
func (s *Struct) Get(field *Field) interface{} {
	value := s.value.Field(field.Num)
	return value.Interface()
}

// GetImmutableFieldsBytes first combine all immutable fields as
// map[string]interface{}
// then pack it via msgpack and return the result
func (s *Struct) GetImmutableFieldsBytes(fields map[string]*Field) []byte {
	if len(fields) == 0 {
		return nil
	}

	combinedFields := map[string]interface{}{}

	for fieldName, field := range fields {
		value := s.value.Field(field.Num)

		combinedFields[fieldName] = value.Interface()
	}

	packedFields, err := msgpack.Marshal(combinedFields)
	if err != nil {
		fmt.Println("GetImmutableFieldsBytes failed:", err)
	}

	return packedFields
}

// GetMutableFieldBytes return mutable field as byteSlice
func (s *Struct) GetMutableFieldBytes(field *Field) []byte {
	value := s.value.Field(field.Num)

	data, err := msgpack.Marshal(value.Interface())
	if err != nil {
		fmt.Println("GetMutableFieldBytes failed:", err)
	}

	return data
}

// getPrimary get primary tuple based on input object
func (s *Struct) getPrimary(object *Object) tuple.Tuple {
	if object.primaryFields == nil {
		object.panic("primary key is undefined")
	}
	return s.getTuple(object.primaryFields)
}

func (s *Struct) getTuple(fields []*Field) tuple.Tuple {
	structTuple := tuple.Tuple{}
	for _, field := range fields {
		fieldVal := s.Get(field)
		structTuple = append(structTuple, field.tupleElement(fieldVal))
	}
	return structTuple
}

// getSubspace get subspace with primary keys for parse object
func (s *Struct) getSubspace(object *Object) subspace.Subspace {
	primaryTuple := s.getPrimary(object)
	return object.primary.Sub(primaryTuple...)
}

func (s *Struct) getType() reflect.Type {
	return reflect.Indirect(s.value).Type()
}

// structEditable return Struct object with check for pointer (could be editable)
func structEditable(data interface{}) *Struct {
	value := reflect.ValueOf(data)
	if value.Kind() != reflect.Ptr {
		panic("you should pass pointer to the object")
	}
	value = value.Elem() // unpointer, interface still
	input := Struct{
		value:    value,
		editable: true,
	}
	return &input
}

// structAny return Struct object from any sruct
func structAny(data interface{}) *Struct {
	value := reflect.ValueOf(data)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	input := Struct{
		value: value,
	}
	return &input
}
