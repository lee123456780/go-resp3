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
	"math/big"
	"strconv"
)

// RedisKind represents the kind of type that a RedisValue represents.
type RedisKind byte

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

// RedisValue represents a Redis command reply value.
type RedisValue interface {
	Kind() RedisKind
	Attr() *Map
	Converter
}

type baseRedisType interface {
	_interface() interface{}
}

var (
	_null           = null{}
	_verbatimString = VerbatimString("")
	_slice          = Slice{}
	_map            = Map{}
	_set            = Set{}
)

var _ baseRedisType = (*null)(nil)
var _ baseRedisType = (*_string)(nil)
var _ baseRedisType = (*number)(nil)
var _ baseRedisType = (*bignumber)(nil)
var _ baseRedisType = (*double)(nil)
var _ baseRedisType = (*boolean)(nil)

var _ RedisValue = (*null)(nil)
var _ RedisValue = (*_string)(nil)
var _ RedisValue = (*number)(nil)
var _ RedisValue = (*bignumber)(nil)
var _ RedisValue = (*double)(nil)
var _ RedisValue = (*boolean)(nil)

var _ RedisValue = (*VerbatimString)(nil)
var _ RedisValue = (*Slice)(nil)
var _ RedisValue = (*Map)(nil)
var _ RedisValue = (*Set)(nil)

var _ RedisValue = (*attrRedisValue)(nil)

type attrRedisValue struct {
	RedisValue
	attr Map
}

func (v attrRedisValue) Attr() *Map { return &v.attr }

// Caution: null need to implement all Slice, Map, Set conversion functions
// --> null is not based on baseExtType but baseType only
type null struct{}

func (n null) _interface() interface{} { return nil }

func (n null) Kind() RedisKind { return RkNull }

func (n null) ToInt64Slice() ([]int64, error)                      { return _slice.ToInt64Slice() }
func (n null) ToSlice() ([]interface{}, error)                     { return _slice.ToSlice() }
func (n null) ToSlice2() ([][]interface{}, error)                  { return _slice.ToSlice2() }
func (n null) ToStringMapSlice() ([]map[string]interface{}, error) { return _slice.ToStringMapSlice() }
func (n null) ToStringSlice() ([]string, error)                    { return _slice.ToStringSlice() }
func (n null) ToTree() ([]interface{}, error)                      { return _slice.ToTree() }
func (n null) ToXrange() ([]XItem, error)                          { return _slice.ToXrange() }
func (n null) ToStringInt64Map() (map[string]int64, error)         { return _map.ToStringInt64Map() }
func (n null) ToStringMap() (map[string]interface{}, error)        { return _map.ToStringMap() }
func (n null) ToStringStringMap() (map[string]string, error)       { return _map.ToStringStringMap() }
func (n null) ToXread() (map[string][]XItem, error)                { return _map.ToXread() }
func (n null) ToStringSet() (map[string]bool, error)               { return _set.ToStringSet() }

type _string string

func (s _string) _interface() interface{} { return string(s) }

func (s _string) Kind() RedisKind { return RkString }

func (s _string) ToString() (string, error)   { return string(s), nil }
func (s _string) ToInt64() (int64, error)     { return strconv.ParseInt(string(s), 10, 64) }
func (s _string) ToFloat64() (float64, error) { return strconv.ParseFloat(string(s), 64) }
func (s _string) ToBool() (bool, error)       { return s == ReplyOK, nil }

type number int64

func (n number) _interface() interface{} { return int64(n) }

func (n number) Kind() RedisKind { return RkNumber }

func (n number) ToString() (string, error)   { return strconv.FormatInt(int64(n), 10), nil }
func (n number) ToInt64() (int64, error)     { return int64(n), nil }
func (n number) ToFloat64() (float64, error) { return float64(n), nil }
func (n number) ToBool() (bool, error)       { return n != 0, nil }

type double float64

func (d double) _interface() interface{} { return float64(d) }

func (d double) Kind() RedisKind { return RkDouble }

func (d double) ToString() (string, error)   { return strconv.FormatFloat(float64(d), 'g', -1, 64), nil }
func (d double) ToFloat64() (float64, error) { return float64(d), nil }
func (d double) ToBool() (bool, error)       { return d != 0, nil }

type bignumber big.Int

func (n *bignumber) _interface() interface{} { return (*big.Int)(n) }

func (n *bignumber) Kind() RedisKind { return RkBigNumber }

func (n *bignumber) ToString() (string, error) { return (*big.Int)(n).String(), nil }
func (n *bignumber) ToInt64() (int64, error) {
	if (*big.Int)(n).IsInt64() {
		return (*big.Int)(n).Int64(), nil
	}
	return 0, newConversionError("ToInt64", n)
}
func (n *bignumber) ToFloat64() (float64, error) {
	if (*big.Int)(n).IsInt64() {
		return float64((*big.Int)(n).Int64()), nil
	}
	return 0, newConversionError("ToFloat64", n)
}
func (n *bignumber) ToBool() (bool, error) { return (*big.Int)(n).Sign() != 0, nil }

type boolean bool

func (b boolean) _interface() interface{} { return bool(b) }

func (b boolean) Kind() RedisKind { return RkBoolean }

func (b boolean) ToString() (string, error) { return strconv.FormatBool(bool(b)), nil }
func (b boolean) ToInt64() (int64, error) {
	if b {
		return 1, nil
	}
	return 0, nil
}
func (b boolean) ToFloat64() (float64, error) {
	if b {
		return 1, nil
	}
	return 0, nil
}
func (b boolean) ToBool() (bool, error) { return bool(b), nil }
