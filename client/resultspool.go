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

var freeResults = make(resultsPool, maxResults)

const (
	maxResults     = 1000
	defResultItems = 1000
)

type resultsPool chan []*result

func (p *resultsPool) get() []*result {
	select {
	case results := <-*p:
		return results
	default:
		return make([]*result, 0, defResultItems)
	}
}

func (p *resultsPool) put(results []*result) {
	results = results[:0]
	select {
	case *p <- results: // added
	default: // full
	}
}
