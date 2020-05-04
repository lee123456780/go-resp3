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

// A VerbatimString represents the redis verbatim string type.
type VerbatimString string

// FileFormat is returning the file format of a verbatim string.
func (s VerbatimString) FileFormat() string { return _verbatimString(s).FileFormat() }

// String implements the Stringer interface.
func (s VerbatimString) String() string { return _verbatimString(s).String() }

// Kind returns the type of a VerbatimString.
func (s VerbatimString) Kind() RedisKind { return _verbatimString(s).Kind() }

// ToString implements the Converter Stringer interface.
func (s VerbatimString) ToString() (string, error) { return _verbatimString(s).ToString() }

// ToInt64 implements the Converter ToInt64er interface.
func (s VerbatimString) ToInt64() (int64, error) { return _verbatimString(s).ToInt64() }

// ToFloat64 implements the Converter ToFloat64er interface.
func (s VerbatimString) ToFloat64() (float64, error) { return _verbatimString(s).ToFloat64() }

// ToBool implements the Converter ToBooler interface.
func (s VerbatimString) ToBool() (bool, error) { return _verbatimString(s).ToBool() }
