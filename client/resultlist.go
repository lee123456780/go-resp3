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
	"sync"
)

var freeResultlist = resultlistPool{}

const maxResultlists = 1000

type resultlistPool struct {
	mu   sync.Mutex
	size int
	free *resultList
}

func (p *resultlistPool) get() *resultList {
	p.mu.Lock()
	if p.free == nil {
		p.mu.Unlock()
		return newResultList()
	}
	p.size--
	list := p.free
	p.free = list.next
	p.mu.Unlock()
	return list
}

func (p *resultlistPool) put(list *resultList) {
	p.mu.Lock()
	if p.size >= maxResultlists {
		p.mu.Unlock()
		return
	}
	list.items = list.items[:0]
	p.size++
	list.next = p.free
	p.free = list
	p.mu.Unlock()
}

type resultList struct {
	items []*result
	next  *resultList
}

func newResultList() *resultList {
	return &resultList{
		items: make([]*result, 0, defResultListItems),
	}
}
