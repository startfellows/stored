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
		fmt.Println("Filling slice with value slice failed: value slice empty or non existing")
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
		setMsgPackSliceObject(obj, value)
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
