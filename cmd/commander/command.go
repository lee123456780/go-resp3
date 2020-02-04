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

package main

import (
	"encoding/json"
)

type commands map[string]*command

type command struct {
	Summary    string      `json:"summary"`
	Complexity string      `json:"complexity"`
	Arguments  []*argument `json:"arguments"`
	Since      string      `json:"since"`
	Group      string      `json:"group"`
}

type vName struct {
	id  string
	ids []string
}

func (v *vName) UnmarshalJSON(b []byte) error {
	if b[0] == '[' { // array
		err := json.Unmarshal(b, &v.ids)
		if len(v.ids) == 1 { // correct def if only one element in array
			v.id = v.ids[0]
			v.ids = nil
		}
		return err
	}
	return json.Unmarshal(b, &v.id)
}

type vType struct {
	id  string
	ids []string
}

func (v *vType) UnmarshalJSON(b []byte) error {
	if b[0] == '[' { // array
		err := json.Unmarshal(b, &v.ids)
		if len(v.ids) == 1 { // correct def if only one element in array
			v.id = v.ids[0]
			v.ids = nil
		}
		return err
	}
	return json.Unmarshal(b, &v.id)
}

type argument struct {
	Command  string   `json:"command"`
	Name     vName    `json:"name"`
	Type     vType    `json:"type"`
	Enum     []string `json:"enum"`
	Optional bool     `json:"optional"`
	Multiple bool     `json:"multiple"` // 1..n
	Variadic bool     `json:"variadic"` // 0..n
}

type argKind int

const (
	akUnknown   = iota
	akEnumConst // enum used as text constant
	akEnumBool  // enum can be represented as bool
	akEnum      // normal enum type
	akStruct    // struct type
	akBasic     // basic type
)

func (a *argument) Kind() argKind {
	if a.Enum != nil {
		switch len(a.Enum) {
		case 1:
			if a.Optional {
				return akEnumBool
			}
			return akEnumConst
		case 2:
			return akEnumBool
		default:
			return akEnum
		}
	}
	if a.Name.ids != nil {
		return akStruct
	}
	return akBasic
}
