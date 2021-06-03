package stored

import "reflect"

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
