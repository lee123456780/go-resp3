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
	"sync/atomic"
	"time"
)

// ErrNotFlushed is returned by a command executed in a unflushed pipeline.
var ErrNotFlushed = errors.New("pipeline: not flushed")

// ErrTimeout is returned by a command after timeout is reached before a async Redis result is available.
var ErrTimeout = errors.New("timeout while waiting for result")

// result represents the internal redis command result (keep external Result clean).
type result interface {
	setErr(err error)
	setTimeout(timeout time.Duration)
	setFlushed(b bool)
	setValue(value RedisValue)
	flush()
	ack()
}

type asyncResult struct {
	done    chan struct{}
	value   RedisValue
	err     error
	timeout time.Duration
	flushed uint32 // if used in pipeline
}

type asyncSubscribeResult struct {
	asyncResult
	cb      MsgCallback
	channel []string
}

type asyncUnsubscribeResult struct {
	asyncResult
	channel []string
}

func newAsyncResult() *asyncResult {
	return &asyncResult{done: make(chan struct{})}
}
func newAsyncSubscribeResult() *asyncSubscribeResult {
	return &asyncSubscribeResult{asyncResult: asyncResult{done: make(chan struct{})}}
}
func newAsyncUnsubscribeResult() *asyncUnsubscribeResult {
	return &asyncUnsubscribeResult{asyncResult: asyncResult{done: make(chan struct{})}}
}

func (r *asyncResult) wait() error {
	if atomic.LoadUint32(&r.flushed) != 1 {
		return ErrNotFlushed
	}
	if r.timeout == 0 {
		<-r.done
		return nil
	}
	t := time.NewTicker(r.timeout)
	defer t.Stop()
	select {
	case <-r.done:
		return nil
	case <-t.C:
		return ErrTimeout
	}
}

func (r *asyncResult) flush() {
	atomic.StoreUint32(&r.flushed, 1)
}

func (r *asyncResult) ack() {
	close(r.done)
}

func (r *asyncResult) setErr(err error) {
	r.err = err
	atomic.StoreUint32(&r.flushed, 1)
	close(r.done)
}

func (r *asyncResult) setTimeout(timeout time.Duration) {
	r.timeout = timeout
}

func (r *asyncResult) setFlushed(b bool) {
	if b {
		r.flushed = 1
	} else {
		r.flushed = 0
	}
}

func (r *asyncResult) setValue(value RedisValue) {
	r.value = value
	close(r.done)
}

// Err returns any of the following errors:
// - <nil>: result received successfully.
// - ErrNotFlushed: result not received yet (pipeline not flushed).
// - ErrTimeout: timeout reached before result is available.
// - RedisError: redis error message if a redis command was executed unsuccessfully.
func (r *asyncResult) Err() error {
	if err := r.wait(); err != nil {
		return err
	}
	return r.err
}

// Kind returns the type of a Redis value.
func (r *asyncResult) Kind() (RedisKind, error) {
	if err := r.wait(); err != nil {
		return RkInvalid, err
	}
	if r.err != nil {
		return RkInvalid, r.err
	}
	return r.value.Kind, nil
}

// Value returns a Redis value.
func (r *asyncResult) Value() (RedisValue, error) {
	if err := r.wait(); err != nil {
		return invalidValue, err
	}
	if r.err != nil {
		return invalidValue, r.err
	}
	return r.value, nil
}

// Attr returns the attribute of a Redis value if provided - <nil> otherwise.
func (r *asyncResult) Attr() (Map, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.value.Attr, nil
}

// IsNull returns <true> if the redis value is null.
func (r *asyncResult) IsNull() (bool, error) {
	if err := r.wait(); err != nil {
		return false, err
	}
	if r.err != nil {
		return false, r.err
	}
	return r.value.IsNull(), nil
}

// VerbatimString returns a VerbatimString if the Redis type is a verbatim string.
func (r *asyncResult) VerbatimString() (VerbatimString, error) {
	if err := r.wait(); err != nil {
		return "", err
	}
	if r.err != nil {
		return "", r.err
	}
	return r.value.VerbatimString()
}

// Slice returns a Slice if the Redis type is an array.
func (r *asyncResult) Slice() (Slice, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.value.Slice()
}

// Map returns a Map if the Redis type is a map.
func (r *asyncResult) Map() (Map, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.value.Map()
}

// Set returns a Set if the Redis type is a set.
func (r *asyncResult) Set() (Set, error) {
	if err := r.wait(); err != nil {
		return nil, err
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.value.Set()
}

// test on Result interface implementation
var _ Result = (*asyncResult)(nil)
var _ Result = (*asyncSubscribeResult)(nil)
var _ Result = (*asyncUnsubscribeResult)(nil)

// test on (internal) result interface implementation
var _ result = (*asyncResult)(nil)
var _ result = (*asyncSubscribeResult)(nil)
var _ result = (*asyncUnsubscribeResult)(nil)

type flushResults []result

func (rs flushResults) setErr(err error) {
	for _, r := range rs {
		r.setErr(err)
	}
}
