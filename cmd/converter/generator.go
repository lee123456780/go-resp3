// SPDX-FileCopyrightText: 2019-2020 Stefan Miller
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"os"
	"strconv"
	"strings"
)

type buffer struct {
	b bytes.Buffer
}

func (b *buffer) write(values ...string) {
	for _, v := range values {
		b.b.WriteString(v)
	}
}

func (b *buffer) writeln(values ...string) {
	b.write(values...)
	b.b.WriteString("\n")
}

func (b *buffer) format() []byte {
	if fmt, err := format.Source(b.b.Bytes()); err == nil {
		return fmt
	}
	return b.b.Bytes()
}

type generator struct {
	b *buffer
}

func newGenerator() *generator {
	return &generator{b: new(buffer)}
}

const header = `// Code generated by %s; DO NOT EDIT.

package %s

`

func (g *generator) writeHeader(pkg string) {
	cmd := "converter"
	if len(os.Args[1:]) != 0 {
		cmd = strings.Join([]string{cmd, strings.Join(os.Args[1:], " ")}, " ")
	}
	g.b.write(fmt.Sprintf(header, strconv.Quote(cmd), pkg))
}

func (g *generator) writeDoc(doc *ast.CommentGroup) {
	for _, comment := range doc.List {
		g.b.writeln(comment.Text)
	}
}

var initialValue = map[string]string{
	"string":         "\"\"",
	"VerbatimString": "\"\"",
	"int64":          "0",
	"float64":        "0",
	"bool":           "false",
}

func typeInitialValue(typ string) string {
	if v, ok := initialValue[typ]; ok {
		return v
	}
	return "nil"
}

func (g *generator) fieldType(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.InterfaceType:
		return "interface{}"
	case *ast.ArrayType:
		return "[]" + g.fieldType(t.Elt)
	case *ast.MapType:
		return "map[" + g.fieldType(t.Key) + "]" + g.fieldType(t.Value)
	}
	return "unknown"
}

func (g *generator) types(fl *ast.FieldList) []string {
	types := make([]string, len(fl.List))

	for i, field := range fl.List {
		types[i] = g.fieldType(field.Type)
	}
	return types
}

const resultTemplate = `func (r *result) %[1]s() (%[2]s) {
	if err := r.wait(); err != nil {
		return %[3]s, err
	}
	return r.value.%[1]s()
}
`

func (g *generator) generateResultFcts(a *analyzer, pkg string) []byte {
	g.b.b.Reset()
	g.writeHeader(pkg)

	for _, field := range a.fields {
		g.writeDoc(field.Doc)
		fctName := field.Names[0].Name

		types := g.types(field.Type.(*ast.FuncType).Results)
		g.b.writeln(fmt.Sprintf(resultTemplate, fctName, strings.Join(types, ", "), typeInitialValue(types[0])))
	}
	return g.b.format()
}

const attrTemplate = `func (%[1]s %[2]s) Attr() *Map { return nil }`
const convertTemplate = `func (%[1]s %[2]s) %[3]s() (%[4]s) {return %[5]s, newConversionError("%[3]s", %[1]s)}`

var objNames = []string{"_null", "_string", "_number", "_double", "_bignumber", "_boolean", "_verbatimString", "_slice", "_map", "_set"}

func (g *generator) objVarname(objName string, objs map[string]map[string]*ast.FuncDecl) string {
	if mths, ok := objs[objName]; ok {
		for _, decl := range mths {
			return decl.Recv.List[0].Names[0].Name
		}
	}
	return "unknown"
}

func (g *generator) objReceiver(objName string, objs map[string]map[string]*ast.FuncDecl) string {
	if mths, ok := objs[objName]; ok {
		for _, decl := range mths {
			if _, star := decl.Recv.List[0].Type.(*ast.StarExpr); star {
				return "*" + objName
			}
			return objName
		}
	}
	return "unknown"
}

func (g *generator) generateRedisValueFcts(a *analyzer, pkg string) []byte {
	g.b.b.Reset()
	g.writeHeader(pkg)

	for _, objName := range objNames {
		if mths, ok := a.objs[objName]; ok {

			varName := g.objVarname(objName, a.objs)
			receiver := g.objReceiver(objName, a.objs)

			g.b.writeln(fmt.Sprintf(attrTemplate, varName, receiver))

			for _, field := range a.fields {
				mthName := field.Names[0].Name
				if _, ok := mths[mthName]; !ok {

					fctName := field.Names[0].Name

					types := g.types(field.Type.(*ast.FuncType).Results)
					g.b.writeln(fmt.Sprintf(convertTemplate, varName, receiver, fctName, strings.Join(types, ", "), typeInitialValue(types[0])))
				}
			}
		}
		g.b.writeln()
	}
	return g.b.format()
}
