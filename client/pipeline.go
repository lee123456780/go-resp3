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
	"bytes"
	"sync"
)

const (
	minResults = 10
	maxResults = 10000
)

// Pipeline supports redis pipelining capabilities.
type Pipeline interface {
	Commands
	Reset()
	Flush() error
}

var _ Pipeline = (*pipeline)(nil)

type pipeline struct {
	mu      sync.Mutex
	c       *conn
	w       *bytes.Buffer
	enc     Encoder
	err     error
	results flushResults
	*command
}

func newPipeline(c *conn) *pipeline {
	p := &pipeline{
		c:       c,
		w:       new(bytes.Buffer),
		results: make(flushResults, 0, minResults),
	}
	p.enc = NewEncoder(p.w)
	p.command = newCommand(&p.mu, p.enc.Encode, p.send, c.sendInterceptor)
	return p
}

func (p *pipeline) send(name string, result result) {
	if p.err != nil {
		result.setErr(p.err)
		p.mu.Unlock()
		return
	}

	err := p.enc.Flush()
	if err != nil {
		p.err = err
		result.setErr(err)
		p.mu.Unlock()
		return
	}

	result.setTimeout(p.c.asyncTimeout)
	//result.setFlushed(false) // pipeline - default value

	p.results = append(p.results, result)
	p.mu.Unlock()
}

func (p *pipeline) Reset() {
	p.err = nil
	p.w.Reset()
	if cap(p.results) > maxResults {
		p.results = make(flushResults, 0, maxResults)
	} else {
		p.results = p.results[:0]
	}
}

func (p *pipeline) Flush() error {
	defer p.Reset()

	if p.err != nil {
		return p.err
	}
	if err := p.c.flushPipeline(p.w, p.results); err != nil {
		return err
	}
	return nil
}
