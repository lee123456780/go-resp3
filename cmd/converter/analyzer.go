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
	"go/ast"
	"sort"
	"strings"
	"unicode"
)

type fct struct {
	recvType    string
	name        string
	doc         []string
	destTypes   []string
	isConverter bool
}

const (
	recvTypeRedisValue  = "RedisValue"
	recvTypeSlice       = "Slice"
	recvTypeMap         = "Map"
	recvTypeSet         = "Set"
	recvTypeAsyncResult = "asyncResult"
)

var recvTypes = map[string]bool{
	recvTypeRedisValue:  true,
	recvTypeSlice:       true,
	recvTypeMap:         true,
	recvTypeSet:         true,
	recvTypeAsyncResult: true,
}

type analyzer struct {
	b    *strings.Builder
	fcts []*fct
}

func newAnalyzer() *analyzer {
	return &analyzer{
		b:    new(strings.Builder),
		fcts: make([]*fct, 0, 25),
	}
}

func (a *analyzer) isExported(fctName string) bool {
	for _, r := range fctName {
		return unicode.IsUpper(r)
	}
	return false
}

func (a *analyzer) isConverter(fctName string) bool {
	return fctName[:2] == "To"
}

func (a *analyzer) getRecvType(node *ast.FuncDecl) (string, bool) {
	if node.Recv.NumFields() != 1 {
		return "", false
	}
	list := node.Recv.List

	name := ""

	switch t := list[0].Type.(type) {
	case *ast.Ident:
		name = t.Name
	case *ast.StarExpr:
		switch t := t.X.(type) {
		case *ast.Ident:
			name = t.Name
		}
	}

	if _, ok := recvTypes[name]; !ok {
		return "", false
	}
	return name, true
}

func (a *analyzer) parseArray(b *strings.Builder, node *ast.ArrayType) {
	b.WriteString("[]")
	a.parseExpr(b, node.Elt)
}

func (a *analyzer) parseMap(b *strings.Builder, node *ast.MapType) {
	b.WriteString("map[")
	a.parseExpr(b, node.Key)
	b.WriteString("]")
	a.parseExpr(b, node.Value)
}

func (a *analyzer) parseExpr(b *strings.Builder, node ast.Expr) {
	switch node := node.(type) {

	case *ast.Ident:
		b.WriteString(node.Name)
	case *ast.InterfaceType:
		b.WriteString("interface{}")
	case *ast.ArrayType:
		a.parseArray(b, node)
	case *ast.MapType:
		a.parseMap(b, node)
	}
}

func (a *analyzer) getDestTypes(node *ast.FuncDecl) ([]string, bool) {
	l := node.Type.Results.NumFields()

	r := make([]string, l)

	if l != 0 {
		for i, field := range node.Type.Results.List {
			a.b.Reset()
			a.parseExpr(a.b, field.Type)
			r[i] = a.b.String()
		}
	}

	return r, true
}

func (a *analyzer) getName(node *ast.FuncDecl) string {
	return node.Name.Name
}

func (a *analyzer) getDoc(node *ast.FuncDecl) []string {
	if node.Doc == nil {
		return nil
	}

	l := len(node.Doc.List)
	r := make([]string, l)

	if l != 0 {
		for i, c := range node.Doc.List {
			r[i] = c.Text
		}
	}
	return r
}

func (a *analyzer) analyze(f *ast.File) bool {

	found := false

	ast.Inspect(f, func(node ast.Node) bool {

		switch node := node.(type) {
		case *ast.FuncDecl:
			recvType, ok := a.getRecvType(node)
			if !ok {
				break
			}
			destTypes, ok := a.getDestTypes(node)
			if !ok {
				break
			}

			name := a.getName(node)

			if a.isExported(name) {
				found = true
				a.fcts = append(a.fcts, &fct{recvType: recvType, name: name, doc: a.getDoc(node), destTypes: destTypes, isConverter: a.isConverter(name)})
			}
		}
		return true
	})

	sort.SliceStable(a.fcts, func(i, j int) bool { return a.fcts[i].name < a.fcts[j].name })
	return found
}
