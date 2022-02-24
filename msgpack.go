package stored

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// checkNilPtrObject check if obj is Nil reflect.Ptr
// If yes, initializes underlying object and return it
func checkNilPtrObject(obj reflect.Value) reflect.Value {
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

// setMsgPackSliceObject setting object with new slice from value
func setMsgPackSliceObject(obj reflect.Value, value interface{}) {
	valueSlice, ok := value.([]interface{})
	if !ok {
		return
	}

	newSlice := reflect.MakeSlice(obj.Type(), 0, len(valueSlice))
	for _, i := range valueSlice {
		sliceItem := reflect.New(obj.Type().Elem())
		setMsgPackObject(sliceItem, i)

		newSlice = reflect.Append(newSlice, reflect.Indirect(sliceItem))
	}

	obj.Set(newSlice)
}

func setMsgPackByteSliceObject(obj reflect.Value, value interface{}) {
	valueSlice, ok := value.([]byte)
	if !ok {
		return
	}

	newSlice := reflect.MakeSlice(obj.Type(), 0, len(valueSlice))
	for _, i := range valueSlice {
		sliceItem := reflect.New(obj.Type().Elem())
		setMsgPackObject(sliceItem, i)

		newSlice = reflect.Append(newSlice, reflect.Indirect(sliceItem))
	}

	obj.Set(newSlice)
}

// setMsgPackStructObject setting structObj as new struct from value
func setMsgPackStructObject(structObj reflect.Value, value interface{}) {
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

		setMsgPackObject(field, fieldData)
	}
}

// setMsgPackMapObject setting mapObject as new map from value
func setMsgPackMapObject(mapObject reflect.Value, value interface{}) {
	valueMap, ok := value.(map[string]interface{})
	if !ok {
		fmt.Println("Filling map with value map failed: value map empty or non existing")
		return
	}

	mapObjectType := mapObject.Type().Elem()
	mapObject.Set(reflect.MakeMap(mapObject.Type()))

	for fieldName, fieldData := range valueMap {
		field := reflect.New(mapObjectType)
		setMsgPackObject(field, fieldData)

		mapObject.SetMapIndex(reflect.ValueOf(fieldName), reflect.Indirect(field))
	}
}

// setMsgPackObjectGround is like setMsgPackObject but when field is predestemined
func setMsgPackObjectGround(field *Field, obj reflect.Value, value interface{}) {
	for {
		if field.Kind != reflect.Ptr {
			break
		}

		obj = checkNilPtrObject(obj)
	}

	switch field.Kind {
	case reflect.Slice:
		setMsgPackSliceObject(obj, value)
	case reflect.Struct:
		setMsgPackStructObject(obj, value)
	case reflect.Map:
		setMsgPackMapObject(obj, value)
	case reflect.Bool:
		switch v := value.(type) {
		case int8:
			if v == 0 {
				obj.SetBool(false)
			} else {
				obj.SetBool(true)
			}
		case bool:
			obj.SetBool(v)
		}
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Uint, reflect.Uint32, reflect.Uint64, reflect.Int8, reflect.Uint8:
		switch v := value.(type) {
		case int8:
			obj.SetInt(int64(v))
		case uint8:
			obj.SetInt(int64(v))
		case int32:
			obj.SetInt(int64(v))
		case uint32:
			obj.SetInt(int64(v))
		case uint64:
			obj.SetInt(int64(v))
		case int64:
			obj.SetInt(v)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			obj.SetString(v)
		}
	default:
		objType := field.Type.Type
		fmt.Println("fieldKIND", field.Kind, "name", field.Name, objType, value)
		obj.Set(reflect.ValueOf(value).Convert(objType))
	}
}

// setMsgPackObject setting obj from value with attempt to cast it to field type
func setMsgPackObject(obj reflect.Value, value interface{}) {
	for {
		if obj.Kind() != reflect.Ptr {
			break
		}

		obj = checkNilPtrObject(obj)
	}

	switch obj.Kind() {
	case reflect.Slice:
		if obj.Type() == reflect.TypeOf([]byte(nil)) {
			setMsgPackByteSliceObject(obj, value)
		} else {
			setMsgPackSliceObject(obj, value)
		}
	case reflect.Struct:
		setMsgPackStructObject(obj, value)
	case reflect.Map:
		setMsgPackMapObject(obj, value)
	default:
		objType := obj.Type()
		obj.Set(reflect.ValueOf(value).Convert(objType))
	}
}

func fillObjectImmutableFields(o *Object, value reflect.Value, data []byte) {
	combinedFields := map[string]interface{}{}
	err := msgpack.Unmarshal(data, &combinedFields)
	if err != nil {
		fmt.Println("Fill failed:", err)
		return
	}

	for fieldName, fieldData := range combinedFields {
		field, fok := o.immutableFields[fieldName]
		if fok {
			fieldObj := value.Field(field.Num)

			setMsgPackObject(fieldObj, fieldData)
		}
	}
}

func GetPlus(kind reflect.Kind) []byte {
	switch kind {
	case reflect.Int, reflect.Int64, reflect.Uint64:
		return []byte{'\x01', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00', '\x00'}
	case reflect.Int32, reflect.Uint32:
		return []byte{'\x01', '\x00', '\x00', '\x00'}
	case reflect.Int16, reflect.Uint16:
		return []byte{'\x01', '\x00'}
	case reflect.Int8, reflect.Uint8:
		return []byte{'\x01'}
	}

	panic("type not supported for plus operation")
}
func GetMinus(kind reflect.Kind) []byte {
	switch kind {
	case reflect.Int, reflect.Int64, reflect.Uint64:
		return []byte{'\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff', '\xff'}
	case reflect.Int32, reflect.Uint32:
		return []byte{'\xff', '\xff', '\xff', '\xff'}
	case reflect.Int16, reflect.Uint16:
		return []byte{'\xff', '\xff'}
	case reflect.Int8, reflect.Uint8:
		return []byte{'\xff'}
	}

	panic("type not supported for minus operation")
}
