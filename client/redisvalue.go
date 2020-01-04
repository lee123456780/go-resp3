/*
Copyright 2019 Stefan Miller

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// RedisKind represents the kind of type that a RedisValue represents.
type RedisKind int

// RedisKind constants.
const (
	RkInvalid RedisKind = iota
	RkError
	RkPush
	RkNull
	RkString
	RkVerbatimString
	RkNumber
	RkDouble
	RkBigNumber
	RkBoolean
	RkSlice
	RkMap
	RkSet
)

// A InvalidValueError indicates an unsuccessful type conversion of a redis value.
// - Name:  Parameter name.
// - Value: Invalid value.
type InvalidValueError struct {
	Name  string
	Value interface{}
}

func newInvalidValueError(name string, value interface{}) *InvalidValueError {
	return &InvalidValueError{Name: name, Value: value}
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("Invalid value %v for %s", e.Value, e.Name)
}

// A RedisError represents the redis error message if a redis command was executed unsuccessfully.
type RedisError struct {
	Code string
	Msg  string
}

func newRedisError(s string) *RedisError {
	p := strings.Split(s, " ")
	return &RedisError{
		Code: p[0],
		Msg:  strings.Join(p[1:], " "),
	}
}

func (e *RedisError) Error() string { return e.Code + " " + e.Msg }

// A VerbatimString represents the redis verbatim string type.
type VerbatimString string

// FileFormat is returning the file format of a verbatim string.
func (v VerbatimString) FileFormat() string { return string(v[:3]) }

// String implements the Stringer interface.
func (v VerbatimString) String() string { return string(v[4:]) }

var zeroInt = new(big.Int)
var invalidValue = RedisValue{RkInvalid, nil, nil}

// A ConversionError indicates an unsuccessful type conversion of a redis value.
// - To:    Name of the conversion function.
// - Value: Value for which the conversion was not successful.
type ConversionError struct {
	To    string
	Value interface{}
}

func newConversionError(to string, value interface{}) *ConversionError {
	return &ConversionError{to, value}
}

func (e *ConversionError) Error() string {
	return fmt.Sprintf("unsupported %s conversion type %T", e.To, e.Value)
}

// RedisValue represents a redis command result value.
type RedisValue struct {
	Kind  RedisKind
	Value interface{}
	Attr  Map
}

// IsNull returns true if RedisKind equals Redis Null Value, false otherwise.
func (v RedisValue) IsNull() bool { return v.Kind == RkNull }

// VerbatimString returns value as VerbatimString.
// If value is not of type VerbatimString a ConversionError is returned.
func (v RedisValue) VerbatimString() (VerbatimString, error) {
	if v, ok := v.Value.(VerbatimString); ok {
		return v, nil
	}
	return "", newConversionError("VerbatimString", v)
}

// Slice returns value as Slice or nil.
// If value is not of type Slice or nil a ConversionError is returned.
func (v RedisValue) Slice() (Slice, error) {
	if v.Value == nil {
		return nil, nil
	}
	if v, ok := v.Value.(Slice); ok {
		return v, nil
	}
	return nil, newConversionError("Slice", v)
}

// Map returns value as Map or nil.
// If value is not of type Map or nil a ConversionError is returned.
func (v RedisValue) Map() (Map, error) {
	if v.Value == nil {
		return nil, nil
	}
	if v, ok := v.Value.(Map); ok {
		return v, nil
	}
	return nil, newConversionError("Map", v)
}

// Set returns value as Set or nil.
// If value is not of type Set or nil a ConversionError is returned.
func (v RedisValue) Set() (Set, error) {
	if v.Value == nil {
		return nil, nil
	}
	if v, ok := v.Value.(Set); ok {
		return v, nil
	}
	return nil, newConversionError("Set", v)
}

// ToString converts a redis value to a string.
// In case the conversion is not supported a ConversionError is returned.
func (v RedisValue) ToString() (string, error) {
	switch v := v.Value.(type) {
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case *big.Int:
		return v.String(), nil
	case bool:
		return strconv.FormatBool(v), nil
	case VerbatimString:
		return string(v)[4:], nil
	default:
		return "", newConversionError("string", v)
	}
}

// ToInt64 converts a redis value to an int64.
// In case the conversion is not supported a ConversionError is returned.
func (v RedisValue) ToInt64() (int64, error) {
	switch v := v.Value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case int64:
		return v, nil
		//	case float64:
		//	case *big.Int:
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case VerbatimString:
		return strconv.ParseInt(string(v)[4:], 10, 64)
	default:
		return 0, newConversionError("int64", v)
	}
}

// ToFloat64 converts a redis value to a float64.
// In case the conversion is not supported a ConversionError is returned.
func (v RedisValue) ToFloat64() (float64, error) {
	switch v := v.Value.(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
		//	case *big.Int:
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case VerbatimString:
		return strconv.ParseFloat(string(v)[4:], 64)
	default:
		return 0, newConversionError("float64", v)
	}
}

// ToBool converts a redis value to a bool.
// In case the conversion is not supported a ConversionError is returned.
func (v RedisValue) ToBool() (bool, error) {
	switch v := v.Value.(type) {
	case string:
		return v == ReplyOK, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case *big.Int:
		return v.Sign() == 0, nil
	case bool:
		return v, nil
	case VerbatimString:
		return string(v)[4:] == "OK", nil
	default:
		return false, newConversionError("bool", v)
	}
}
