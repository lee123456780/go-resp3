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
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

// ErrNotFlushed is returned by a command executed in a unflushed pipeline.
var ErrNotFlushed = errors.New("pipeline: not flushed")

// ErrTimeout is returned by a command after timeout is reached before a async Redis result is available.
var ErrTimeout = errors.New("timeout while waiting for result")

// Result represents a redis command result.
type Result interface {
	// Attr returns the attribute of a Redis value if provided - <nil> otherwise.
	Attr() (*Map, error)
	// Err returns any of the following errors:
	// - <nil>: result received successfully.
	// - ErrNotFlushed: result not received yet (pipeline not flushed).
	// - ErrTimeout: timeout reached before result is available.
	// - RedisError: redis error message if a redis command was executed unsuccessfully.
	Err() error
	// IsNull returns <true> if the redis value is null.
	IsNull() (bool, error)
	// Kind returns the type of a Redis value.
	Kind() (RedisKind, error)
	// Value returns a Redis value.
	Value() (RedisValue, error)
	// VerbatimString returns a VerbatimString if the Redis type is a verbatim string.
	VerbatimString() (VerbatimString, error)
	// Map returns a Map if the Redis type is a map.
	Map() (Map, error)
	// Set returns a Set if the Redis type is a set.
	Set() (Set, error)
	// Slice returns a Slice if the Redis type is an array.
	Slice() (Slice, error)

	// Conversion methods.
	Converter
}

const (
	rsNotFlushed uint32 = 0
	rsFlushed    uint32 = 1 << iota
	rsAvailable
	rsWaiting
	rsSetting
)

type result struct {
	// caution: field alignment (memory consumption)
	value   RedisValue
	err     error
	request *request
	flags   uint32
}

func newResult() *result {
	return &result{request: freeRequest.get()}
}

func (r *result) cmd() []interface{} {
	return r.request.cmd
}

// set error before flush like InvalidValueError
func (r *result) setErr(err error) {
	// check on not flushed
	if atomic.LoadUint32(&r.flags) != rsNotFlushed {
		panic("cannot set error for result in not flushed state")
	}
	r.err = err
	atomic.StoreUint32(&r.flags, rsAvailable)
}

func (r *result) flush() {
	atomic.StoreUint32(&r.flags, rsFlushed)
}

func (r *result) wait() error {
	if atomic.LoadUint32(&r.flags) == rsNotFlushed {
		return ErrNotFlushed
	}

	for !atomic.CompareAndSwapUint32(&r.flags, rsFlushed, rsWaiting) {
		if atomic.LoadUint32(&r.flags) == rsAvailable {
			return r.err
		}
		runtime.Gosched() // spinning - allow other goroutines to process
	}

	// wait for done signal
	<-r.request.done
	if atomic.LoadUint32(&r.flags) != rsAvailable {
		panic("result - inconsistent state")
	}
	return r.err
}

func (r *result) ack(value RedisValue, err error) {
	// todo - state type + check state

	isWaiting := !atomic.CompareAndSwapUint32(&r.flags, rsFlushed, rsSetting)
	r.value = value
	r.err = err
	atomic.StoreUint32(&r.flags, rsAvailable)
	if isWaiting {
		r.request.done <- true
	}
	// free data
	freeRequest.put(r.request)
	r.request = nil
}

func (r *result) setTimeout(timeout time.Duration) {
	r.request.timeout = timeout
}

// Err returns any of the following errors:
// - <nil>: result received successfully.
// - ErrNotFlushed: result not received yet (pipeline not flushed).
// - ErrTimeout: timeout reached before result is available.
// - RedisError: redis error message if a redis command was executed unsuccessfully.
func (r *result) Err() error {
	if err := r.wait(); err != nil {
		return err
	}
	return r.err
}

// Kind returns the type of a Redis value.
func (r *result) Kind() (RedisKind, error) {
	if err := r.wait(); err != nil {
		return RkInvalid, err
	}
	return r.value.Kind(), nil
}

// Value returns a Redis value.
func (r *result) Value() (RedisValue, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	return r.value, nil
}

// Attr returns the attribute of a Redis value if provided - <nil> otherwise.
func (r *result) Attr() (*Map, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	return r.value.Attr(), nil
}

// IsNull returns <true> if the redis value is null.
func (r *result) IsNull() (bool, error) {
	if err := r.wait(); err != nil {
		return false, err
	}
	return r.value.Kind() == RkNull, nil
}

// VerbatimString returns a VerbatimString if the Redis type is a verbatim string.
func (r *result) VerbatimString() (VerbatimString, error) {
	if err := r.wait(); err != nil {
		return _verbatimString, err
	}
	s, ok := r.value.(VerbatimString)
	if !ok {
		return _verbatimString, newConversionError("VerbatimString", r.value)
	}
	return s, nil
}

// Slice returns a Slice if the Redis type is an array.
func (r *result) Slice() (Slice, error) {
	if err := r.wait(); err != nil {
		return _slice, err
	}
	switch r.value.Kind() {
	case RkSlice:
		return r.value.(Slice), nil
	case RkNull:
		return _slice, nil
	default:
		return _slice, newConversionError("Slice", r.value)
	}
}

// Map returns a Map if the Redis type is a map.
func (r *result) Map() (Map, error) {
	if err := r.wait(); err != nil {
		return _map, err
	}
	switch r.value.Kind() {
	case RkMap:
		return r.value.(Map), nil
	case RkNull:
		return _map, nil
	default:
		return _map, newConversionError("Map", r.value)
	}
}

// Set returns a Set if the Redis type is a set.
func (r *result) Set() (Set, error) {
	if err := r.wait(); err != nil {
		return _set, err
	}
	switch r.value.Kind() {
	case RkSet:
		return r.value.(Set), nil
	case RkNull:
		return _set, nil
	default:
		return _set, newConversionError("Set", r.value)
	}
}

// test on Result interface implementation
var _ Result = (*result)(nil)
