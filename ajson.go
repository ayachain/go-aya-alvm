package lua

import (
	"encoding/json"
	"errors"
)

// Preload adds json to the given Lua state's package.preload table. After it
// has been preloaded, it can be loaded using require:
//
//  local json = require("json")
func AJsonPreload(L *LState) {
	L.PreloadModule("json", AJsonLoader)
}

// Loader is the module loader function.
func AJsonLoader(L *LState) int {
	t := L.NewTable()
	L.SetFuncs(t, api)
	L.Push(t)
	return 1
}

var api = map[string]LGFunction{
	"decode": apiDecode,
	"encode": apiEncode,
}

func apiDecode(L *LState) int {
	str := L.CheckString(1)

	value, err := L.Decode([]byte(str))
	if err != nil {
		L.Push(LNil)
		L.Push(LString(err.Error()))
		return 2
	}
	L.Push(value)
	return 1
}

func apiEncode(L *LState) int {
	value := L.CheckAny(1)

	data, err := Encode(value)
	if err != nil {
		L.Push(LNil)
		L.Push(LString(err.Error()))
		return 2
	}
	L.Push(LString(string(data)))
	return 1
}

var (
	errNested      = errors.New("cannot encode recursively nested tables to JSON")
	errSparseArray = errors.New("cannot encode sparse array")
	errInvalidKeys = errors.New("cannot encode mixed or invalid key types")
)

type invalidTypeError LValueType

func (i invalidTypeError) Error() string {
	return `cannot encode ` + LValueType(i).String() + ` to JSON`
}

// Encode returns the JSON encoding of value.
func Encode(value LValue) ([]byte, error) {
	return json.Marshal(jsonValue{
		LValue:  value,
		visited: make(map[*LTable]bool),
	})
}

type jsonValue struct {
	LValue
	visited map[*LTable]bool
}

func (j jsonValue) MarshalJSON() (data []byte, err error) {
	switch converted := j.LValue.(type) {
	case LBool:
		data, err = json.Marshal(bool(converted))
	case LNumber:
		data, err = json.Marshal(float64(converted))
	case *LNilType:
		data = []byte(`null`)
	case LString:
		data, err = json.Marshal(string(converted))
	case *LTable:
		if j.visited[converted] {
			return nil, errNested
		}
		j.visited[converted] = true

		key, value := converted.Next(LNil)

		switch key.Type() {
		case LTNil: // empty table
			data = []byte(`[]`)
		case LTNumber:
			arr := make([]jsonValue, 0, converted.Len())
			expectedKey := LNumber(1)
			for key != LNil {
				if key.Type() != LTNumber {
					err = errInvalidKeys
					return
				}
				if expectedKey != key {
					err = errSparseArray
					return
				}
				arr = append(arr, jsonValue{value, j.visited})
				expectedKey++
				key, value = converted.Next(key)
			}
			data, err = json.Marshal(arr)
		case LTString:
			obj := make(map[string]jsonValue)
			for key != LNil {
				if key.Type() != LTString {
					err = errInvalidKeys
					return
				}
				obj[key.String()] = jsonValue{value, j.visited}
				key, value = converted.Next(key)
			}
			data, err = json.Marshal(obj)
		default:
			err = errInvalidKeys
		}
	default:
		err = invalidTypeError(j.LValue.Type())
	}
	return
}

// Decode converts the JSON encoded data to Lua values.
func ( l *LState ) Decode(data []byte) (LValue, error) {

	var value interface{}
	err := json.Unmarshal(data, &value)
	if err != nil {
		return nil, err
	}
	return l.DecodeValue(value), nil
}

// DecodeValue converts the value to a Lua value.
//
// This function only converts values that the encoding/json package decodes to.
// All other values will return lua.LNil.
func ( l *LState ) DecodeValue( value interface{}) LValue {
	switch converted := value.(type) {
	case bool:
		return LBool(converted)
	case float64:
		return LNumber(converted)
	case string:
		return LString(converted)
	case json.Number:
		return LString(converted)
	case []interface{}:
		arr := l.CreateTable(len(converted), 0)
		for _, item := range converted {
			arr.Append(l.DecodeValue(item))
		}
		return arr
	case map[string]interface{}:
		tbl := l.CreateTable(0, len(converted))
		for key, item := range converted {
			tbl.RawSetH(LString(key), l.DecodeValue(item))
		}
		return tbl
	case nil:
		return LNil
	}

	return LNil
}
