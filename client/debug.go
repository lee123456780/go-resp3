// +build debug

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
	"reflect"
)

func reflectStruct(t reflect.Type) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		fmt.Printf("Field %s size %d align %d offset %d\n", f.Name, f.Type.Size(), f.Type.Align(), f.Offset)
	}
}

func reflectType(t reflect.Type) {
	switch t.Kind() {

	case reflect.Struct:
		fmt.Printf("Struct %s size %d align %d\n", t.Name(), t.Size(), t.Align())
		reflectStruct(t)
	}
}

func init() {
	reflectType(reflect.TypeOf(result{}))
	reflectType(reflect.TypeOf(request{}))
}
