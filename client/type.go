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
	"math"
	"strconv"
)

var (
	// InfPos can be used as infinite positive value for Zfloat64 parameters.
	InfPos = Zclose(math.Inf(+1))
	// InfNeg can be used as infinite negative value for Zfloat64 parameters.
	InfNeg = Zclose(math.Inf(-1))
)

// Zfloat64 is the float64 used in some sorted sets commands for min and max values.
type Zfloat64 interface {
	zfloat64()
	String() string
}

// Zopen is the open interval Zfloat64 type.
type Zopen float64

func (z Zopen) String() string {
	return "(" + strconv.FormatFloat(float64(z), 'g', -1, 64)
}

// Zclose is the close interval Zfloat64 type.
type Zclose float64

func (z Zclose) String() string {
	return strconv.FormatFloat(float64(z), 'g', -1, 64)
}

// Zfloat64 marker methods
func (z Zopen) zfloat64()  {}
func (z Zclose) zfloat64() {}
