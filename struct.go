package stored

import (
	"bytes"
	"encoding/binary"
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

// setFieldAutoIncr sets auto incr field value using bytes
func (s *Struct) setFieldAutoIncr(field *Field, data []byte) {
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

	var endian = binary.LittleEndian

	reader := bytes.NewReader(data)
	err := binary.Read(reader, endian, objField.Addr().Interface())
	if err != nil {
		fmt.Println("Decode to value failed", field.Name, field.object.name, len(data), err)
	}

	objField.Set(reflect.Indirect(objField))
}

// incField increment field value using interface value
func (s *Struct) incField(field *Field, toInc interface{}) {
	objField := s.value.Field(field.Num)
	inter := objField.Interface()
	switch field.Kind {
	case reflect.Int:
		s.setObject(objField, inter.(int)+toInc.(int))
	case reflect.Uint:
		s.setObject(objField, inter.(uint)+toInc.(uint))
	case reflect.Int32:
		convertInc, ok := toInc.(int32)
		if !ok {
			convertInc = int32(toInc.(int))
		}
		s.setObject(objField, inter.(int32)+convertInc)
	case reflect.Uint32:
		s.setObject(objField, inter.(uint32)+toInc.(uint32))
	case reflect.Int64:
		s.setObject(objField, inter.(int64)+toInc.(int64))
	case reflect.Uint64:
		s.setObject(objField, inter.(uint64)+toInc.(uint64))
	default:
		panic("field " + field.Name + " is not incrementable")
	}
}

// checkNilPtrObject check if obj is Nil reflect.Ptr
// If yes, initializes underlying object and return it
func (s *Struct) checkNilPtrObject(obj reflect.Value) reflect.Value {
	if obj.Kind() == reflect.Ptr {
		if obj.IsNil() {
			t := obj.Type().Elem()
			newValue := reflect.New(t)

			obj.Set(newValue)
		}

		return reflect.Indirect(obj)
	}

	return obj
}

// setSliceObject setting object with new slice from value
func (s *Struct) setSliceObject(obj reflect.Value, value interface{}) {
	valueSlice, ok := value.([]interface{})
	if !ok {
		fmt.Println("Filling slice with value slice failed: value slice empty or non existing")
		return
	}

	newSlice := reflect.MakeSlice(obj.Type(), 0, len(valueSlice))
	for _, i := range valueSlice {
		sliceItem := reflect.New(obj.Type().Elem())
		s.setObject(sliceItem, i)

		newSlice = reflect.Append(newSlice, reflect.Indirect(sliceItem))
	}

	obj.Set(newSlice)
}

// setStructObject setting structObj as new struct from value
func (s *Struct) setStructObject(structObj reflect.Value, value interface{}) {
	valueMap, ok := value.(map[string]interface{})
	if !ok {
		fmt.Println("Filling struct with value map failed: value map empty or non existing")
		return
	}

	for fieldName, fieldData := range valueMap {
		field := structObj.FieldByName(fieldName)
		if reflect.ValueOf(field).IsZero() {
			continue
		}

		s.setObject(field, fieldData)
	}
}

// setMapObject setting mapObject as new map from value
func (s *Struct) setMapObject(mapObject reflect.Value, value interface{}) {
	valueMap, ok := value.(map[string]interface{})
	if !ok {
		fmt.Println("Filling map with value map failed: value map empty or non existing")
		return
	}

	mapObjectType := mapObject.Type().Elem()
	mapObject.Set(reflect.MakeMap(mapObject.Type()))

	for fieldName, fieldData := range valueMap {
		field := reflect.New(mapObjectType)
		s.setObject(field, fieldData)

		mapObject.SetMapIndex(reflect.ValueOf(fieldName), reflect.Indirect(field))
	}
}

// setObject setting obj from value with attempt to cast it to field type
func (s *Struct) setObject(obj reflect.Value, value interface{}) {
	for {
		if obj.Kind() != reflect.Ptr {
			break
		}

		obj = s.checkNilPtrObject(obj)
	}

	switch obj.Kind() {
	case reflect.Slice:
		s.setSliceObject(obj, value)
	case reflect.Struct:
		s.setStructObject(obj, value)
	case reflect.Map:
		s.setMapObject(obj, value)
	default:
		objType := obj.Type()
		obj.Set(reflect.ValueOf(value).Convert(objType))
	}
}

// Fill will use data inside value object to fill struct
func (s *Struct) Fill(o *Object, v *Value) {
	if !s.editable {
		panic("attempt to change readonly struct")
	}

	for fieldName, binaryValue := range v.raw {
		field, ok := o.mutableFields[fieldName]
		if ok {
			s.setField(field, binaryValue)
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

	immutableFieldsData, ok := v.raw["*"]
	if ok {
		fillObjectImmutableFields(o, s.value, immutableFieldsData)
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
	//if field.SimpleType()
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
