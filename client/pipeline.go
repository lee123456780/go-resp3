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
	"sync/atomic"
)

// Pipeline supports redis pipelining capabilities.
// Multiple goroutines must not invoke methods on a Pipeline simultaneously.
type Pipeline interface {
	Commands
	Reset()
	Flush() error
}

var _ Pipeline = (*pipeline)(nil)

type pipeline struct {
	c    *conn
	err  error
	list *resultList
	*command
}

func newPipeline(c *conn) *pipeline {
	p := &pipeline{c: c, list: freeResultlist.get()}
	p.command = newCommand(p.send, c.sendInterceptor)
	return p
}

func (p *pipeline) send(name string, r *result) {
	if atomic.LoadUint32(&p.c.inShutdown) != 0 {
		r.setErr(ErrInShutdown)
		return
	}
	p.list.items = append(p.list.items, r)
}

func (p *pipeline) Reset() {
	p.list.items = p.list.items[:0]
}

func (p *pipeline) Flush() error {
	err := p.c.flush(true, p.list)
	p.list = freeResultlist.get()
	return err
}
