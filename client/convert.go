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
)

// A ConversionError is raised for an unsuccessful type conversion of a redis value.
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

// Converter is the interface that groups Redis value conversion interfaces.
type Converter interface {
	Stringer
	Int64er
	Float64er
	Booler

	Int64Slicer
	Slicer
	Slice2er
	StringMapSlicer
	StringSlicer
	Treer
	Xranger

	StringMapper
	StringInt64Mapper
	StringStringMapper
	Xreader

	StringSetter
}

// Stringer is implemented by any redis value that has a ToString method.
type Stringer interface {
	// ToString converts a redis value to a string.
	// In case the conversion is not supported a ConversionError is returned.
	ToString() (string, error)
}

// Int64er is implemented by any redis value that has a ToInt64 method.
type Int64er interface {
	// ToInt64 converts a redis value to an int64.
	// In case the conversion is not supported a ConversionError is returned.
	ToInt64() (int64, error)
}

// Float64er is implemented by any redis value that has a ToFloat64 method.
type Float64er interface {
	// ToFloat64 converts a redis value to a float64.
	// In case the conversion is not supported a ConversionError is returned.
	ToFloat64() (float64, error)
}

// Booler is implemented by any redis value that has a ToBool method.
type Booler interface {
	// ToBooler converts a redis value to a bool.
	// In case the conversion is not supported a ConversionError is returned.
	ToBool() (bool, error)
}

// Int64Slicer is implemented by any redis value that has a ToInt64Slice method.
type Int64Slicer interface {
	// ToInt64Slice returns a slice with values of type int64. In case value conversion to string is not possible
	// a ConversitionError is returned.
	ToInt64Slice() ([]int64, error)
}

// Slicer is implemented by any redis value that has a ToSlice method.
type Slicer interface {
	// ToSlice returns a slice with values of type interface{}.
	ToSlice() ([]interface{}, error)
}

// Slice2er is implemented by any redis value that has a ToSlice2 method.
type Slice2er interface {
	// ToSlice2 returns a slice with values of type []interface{}. In case value conversion to []interface{} is not possible
	// a ConversitionError is returned.
	ToSlice2() ([][]interface{}, error)
}

// StringMapSlicer is implemented by any redis value that has a ToStringMapSlice method.
type StringMapSlicer interface {
	// ToStringMapSlice returns a slice with values of type map[string]interfcae{}. In case value conversion to map[string]interface{} is not possible
	// a ConversitionError is returned.
	ToStringMapSlice() ([]map[string]interface{}, error)
}

// StringSlicer is implemented by any redis value that has a ToStringSlice method.
type StringSlicer interface {
	// ToStringSlice returns a slice with values of type string. In case value conversion to string is not possible
	// a ConversitionError is returned.
	ToStringSlice() ([]string, error)
}

// Treer is implemented by any redis value that has a ToTree method.
type Treer interface {
	// ToTree returns a tree with nodes of type []interface{} and leaves of type interface{}. In case value conversion to []interface{} is not possible
	// a ConversitionError is returned.
	ToTree() ([]interface{}, error)
}

// Xranger is implemented by any redis value that has a ToXrange method.
type Xranger interface {
	// ToXrange returns a slice with values of type IdMap. In case the conversion is not possible
	// a ConversitionError is returned.
	ToXrange() ([]IDMap, error)
}

// StringInt64Mapper is implemented by any redis value that has a ToStringInt64Map method.
type StringInt64Mapper interface {
	// ToStringInt64Map returns a map with keys of type string and values of type int64. In case key or value conversion is not possible
	// a ConvertionError is returned.
	ToStringInt64Map() (map[string]int64, error)
}

// StringMapper is implemented by any redis value that has a ToStringMap method.
type StringMapper interface {
	// ToStringMap returns a map with keys of type string. In case key conversion to string is not possible
	// a ConvertionError is returned.
	ToStringMap() (map[string]interface{}, error)
}

// StringStringMapper is implemented by any redis value that has a ToStringStringMap method.
type StringStringMapper interface {
	// ToStringStringMap returns a map with keys and values of type string. In case key or value conversion to string is not possible
	// a ConvertionError is returned.
	ToStringStringMap() (map[string]string, error)
}

// Xreader is implemented by any redis value that has a ToXread method.
type Xreader interface {
	// ToXread returns a map[string] with values of type IdMap. In case the conversion is not possible
	// a ConversitionError is returned.
	ToXread() (map[string][]IDMap, error)
}

// StringSetter is implemented by any redis value that has a ToStringSet method.
type StringSetter interface {
	// ToStringSet returns a map with keys of type string and boolean true values. In case key conversion to string is not possible
	// a ConvertionError is returned.
	ToStringSet() (map[string]bool, error)
}
