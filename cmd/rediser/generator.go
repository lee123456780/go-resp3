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
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"go-resp3/cmd/rediser/internal/ast"
	"go-resp3/cmd/rediser/internal/stringutils"
)

const (
	intfName   = "Commands"
	callSetErr = "r.setErr"
)

type sorter struct {
	key   string
	value string
}

type buffer struct {
	b      bytes.Buffer
	indent int
}

// generic write methods
func (b *buffer) startBlock(values ...string) {
	b.write(values...)
	b.writeln(" {")
	b.indent++
}

func (b *buffer) endBlock(values ...string) {
	b.indent--
	b.write("}")
	b.writeln(values...)
}

func (b *buffer) endStartBlock(values ...string) {
	b.indent--
	b.write("} ")
	b.startBlock(values...)
}

func (b *buffer) startDef(values ...string) {
	b.write(values...)
	b.writeln(" (")
	b.indent++
}

func (b *buffer) endDef() {
	b.indent--
	b.writeln(")")
}

func (b *buffer) startInit(values ...string) {
	b.write(values...)
	b.writeln(" {")
	b.indent++
}

func (b *buffer) endInit(values ...string) {
	b.indent--
	b.write("}")
	b.writeln(values...)
}

func (b *buffer) write(values ...string) {
	b.b.WriteString(strings.Repeat("\t", b.indent))
	for _, v := range values {
		b.b.WriteString(v)
	}
}

func (b *buffer) writeln(values ...string) {
	b.write(values...)
	b.b.WriteString("\n")
}

func (b *buffer) comment(values ...string) {
	b.b.WriteString(strings.Repeat("\t", b.indent))
	b.b.WriteString("// ")
	b.write(values...)
}

func (b *buffer) commentln(values ...string) {
	b.comment(values...)
	b.b.WriteString("\n")
}

// special write methods
func (b *buffer) add(arg string) {
	b.writeln("c.encode(", arg, ")")
}

func (b *buffer) setInvalidValueError(name, value string) {
	b.writeln(callSetErr, "(newInvalidValueError(", name, ", ", value, "))")
}

type generator struct {
	b *buffer
	s *ast.Scope
}

func newGenerator(s *ast.Scope) *generator {
	return &generator{
		b: new(buffer),
		s: s,
	}
}

func (g *generator) generateEnums() {
	g.s.LoopEnum(func(decl *ast.EnumDecl) {
		g.b.writeln("type ", decl.Name, " string")
		g.b.startDef("const")
		for _, v := range decl.Values {
			g.b.writeln(decl.Name, stringutils.PascalCase(v), " ", decl.Name, " = ", strconv.Quote(v))
		}
		g.b.endDef()
	})
}

func (g *generator) generateStructs() {
	g.s.LoopStruct(func(decl *ast.StructDecl) {
		g.b.startBlock("type ", decl.Name, " struct")
		decl.List.WalkNode(func(level int, node ast.FieldNode) {
			g.b.writeln(node.NodeName(), " ", node.NodeType().String())
		})
		g.b.endBlock()
	})
}

func (g *generator) generateSignature(config *ast.FuncConfig, fields ast.FieldList) {
	g.b.write("(")

	first := true
	lastType := ""

	fields.WalkNode(func(level int, node ast.FieldNode) {
		if node.NodeType() != nil && level == 0 {
			actType := node.NodeType().String()
			if first {
				first = false
				lastType = actType
			} else {
				if lastType != actType {
					g.b.write(" ", lastType)
					lastType = actType
				}
				g.b.write(", ")
			}
			g.b.write(node.NodeName())
		}
	})
	if lastType != "" {
		g.b.write(" ", lastType)
	}
	if config != nil {
		if callback, ok := config.Config[ast.ConfigCallback]; ok {
			if !first {
				g.b.write(", ")
			}
			g.b.write("cb ", callback)
		}
	}
	g.b.write(")")
}

func (g *generator) generateInterfaces(groupIdx []groupIdx) {
	g.b.startBlock("type ", intfName, " interface")
	for _, e := range groupIdx {
		g.b.writeln(e.key, intfName)
	}
	g.b.endBlock()

	for _, e := range groupIdx {
		g.b.startBlock("type ", e.key, intfName, " interface")
		for _, decl := range e.decls {
			g.b.write(decl.Name)
			config := g.s.LookupFuncConfig(decl.Name)
			g.generateSignature(config, decl.List)
			g.b.writeln(" Result")
		}
		g.b.endBlock()
	}
}

func (g *generator) generateBaseType(name, cmd string, fieldType *ast.BaseType) {
	if cmd != "" {
		g.b.add(strconv.Quote(cmd))
	}
	g.b.add(name)
}

func (g *generator) generateDataType(name, cmd string, fieldType *ast.DataType) {
	if cmd != "" {
		g.b.add(strconv.Quote(cmd))
	}

	decl := g.s.Lookup(fieldType.Name)

	switch decl := decl.(type) {

	case *ast.EnumDecl:
		g.b.add(name)

	case *ast.StructDecl:
		decl.List.WalkNode(func(level int, node ast.FieldNode) {
			g.generateField(name+"."+node.NodeName(), node.NodeCmd(), true, false, node.NodeType())
		})
	}
}

func (g *generator) generateEnumBoolType(name, cmd string, ptr bool, fieldType *ast.EnumBoolType) {
	if ptr {
		g.b.startBlock("if *", name)
	} else {
		g.b.startBlock("if ", name)
	}
	switch len(fieldType.Values) {
	case 1:
		g.generateField(strconv.Quote(fieldType.Values[0]), cmd, true, false, fieldType.BaseType())
	case 2:
		g.generateField(strconv.Quote(fieldType.Values[0]), cmd, true, false, fieldType.BaseType())
		g.b.endStartBlock("else")
		g.generateField(strconv.Quote(fieldType.Values[1]), cmd, true, false, fieldType.BaseType())
	default:
		panic("wrong number of values")
	}
	g.b.endBlock()
}

func (g *generator) generatePointerType(name, cmd string, nilTest bool, fieldType ast.TypeNode) {
	if nilTest {
		g.b.startBlock("if ", name, " != nil")
		defer g.b.endBlock()
	}
	if cmd != "" {
		g.b.add(strconv.Quote(cmd))
	}
	g.generateField(name, "", true, true, fieldType)
}

func (g *generator) generateSliceType(name, cmd string, allowNil bool, fieldType ast.TypeNode) {
	if allowNil && cmd != "" {
		g.b.startBlock("if ", name, " != nil")
		defer g.b.endBlock()
	}
	if cmd != "" {
		g.b.add(strconv.Quote(cmd))
	}
	g.b.startBlock("for _, v := range ", name)
	g.generateField("v", "", true, false, fieldType)
	g.b.endBlock()
}

func (g *generator) generateEllipsisType(name, cmd string, fieldType ast.TypeNode) {
	if cmd != "" {
		g.b.add(strconv.Quote(cmd))
	}
	g.b.add(name + "...")
}

func (g *generator) generateField(name, cmd string, nilTest, ptr bool, node ast.TypeNode) {
	switch node := node.(type) {

	case *ast.PointerType:
		g.generatePointerType(name, cmd, nilTest, node.Node)
	case *ast.SliceType:
		g.generateSliceType(name, cmd, node.AllowNil, node.Node)
	case *ast.EllipsisType:
		g.generateEllipsisType(name, cmd, node.Node)
	case *ast.BaseType:
		g.generateBaseType(name, cmd, node)
	case *ast.DataType:
		g.generateDataType(name, cmd, node)
	case *ast.EnumBoolType:
		g.generateEnumBoolType(name, cmd, ptr, node)
	default:
		g.b.add(strconv.Quote(cmd)) // no type -> constant
	}
}

func (g *generator) generateFieldNode(node ast.FieldNode) {
	switch node := node.(type) {
	case *ast.Alternative:
		if node.Cmd != "" {
			g.b.add(strconv.Quote(node.Cmd))
		}
		_, slice := node.Type.(*ast.SliceType)
		if slice {
			g.b.startBlock("for _, v := range ", node.Name)
			defer g.b.endBlock()
			g.b.startBlock("switch v := v.(type)")
		} else {
			g.b.startBlock("switch v := ", node.Name, ".(type)")
		}
		node.List.WalkNode(func(level int, node ast.FieldNode) {
			g.b.writeln("case ", node.NodeType().String(), ":")
			g.generateField("v", node.NodeCmd(), false, false, node.NodeType())
		})
		g.b.writeln("default:")
		g.b.setInvalidValueError(strconv.Quote(node.Name), "v")
		g.b.endBlock()
	default:
		g.generateField(node.NodeName(), node.NodeCmd(), true, false, node.NodeType())
	}
}

func (g *generator) generateFieldCheck(name string, typ ast.TypeNode) {
	switch v := typ.(type) {

	case *ast.SliceType:
		if !v.AllowNil {
			g.b.startBlock("if ", name, " == nil")
			g.b.setInvalidValueError(strconv.Quote(name), "nil")
			g.b.writeln("return r")
			g.b.endBlock()
		}
	}
}

func (g *generator) generateMethod(config *ast.FuncConfig, decl *ast.FuncDecl) {

	decl.List.WalkNode(func(level int, node ast.FieldNode) {
		if level == 0 {
			g.generateFieldCheck(node.NodeName(), node.NodeType())
		}
	})

	if config != nil {
		switch config.Config[ast.ConfigType] {
		case ast.ConfigTypeSubscribe:
			g.b.writeln("r.channel = ", config.Config[ast.ConfigChannel], "[:]")
			g.b.writeln("r.cb = cb")
		case ast.ConfigTypeUnsubscribe:
			g.b.writeln("r.channel = ", config.Config[ast.ConfigChannel], "[:]")
		default:
			panic("invalid function configuration type")
		}
	}

	g.b.writeln("c.mu.Lock()")

	for _, token := range decl.Token {
		g.b.add(strconv.Quote(token))
	}

	decl.List.WalkNode(func(level int, node ast.FieldNode) {
		if level == 0 {
			g.generateFieldNode(node)
		}
	})
}

func (g *generator) generateMethods() {

	g.s.LoopFunc(func(decl *ast.FuncDecl) {
		attr := g.s.LookupFuncAttr(decl.Attr)
		if attr == nil {
			panic("function attributes (group) not found: " + decl.Attr) // should never happen
		}
		config := g.s.LookupFuncConfig(decl.Name)
		g.b.commentln(decl.Name, " - ", attr.Summary)
		g.b.commentln("Group: ", attr.Group)
		g.b.commentln("Since: ", attr.Since)
		if attr.Complexity != "" {
			if len(attr.Complexity) <= 80 {
				g.b.commentln("Complexity: ", attr.Complexity)
			} else {
				g.b.commentln("Complexity: ")
				parts := stringutils.Split(attr.Complexity, 80)
				for _, part := range parts {
					g.b.commentln(part)
				}
			}
		}

		g.b.write("func (c *command) ", decl.Name)
		g.generateSignature(config, decl.List)
		g.b.startBlock(" Result")

		if config != nil {
			switch config.Config[ast.ConfigType] {
			case ast.ConfigTypeSubscribe:
				g.b.writeln("r := newAsyncSubscribeResult()")
			case ast.ConfigTypeUnsubscribe:
				g.b.writeln("r := newAsyncUnsubscribeResult()")
			default:
				panic("ivalid function configuration type")
			}
		} else {
			g.b.writeln("r := newAsyncResult()")
		}

		g.generateMethod(config, decl)
		g.b.writeln("c.send(Cmd", decl.Name, ", r)")
		g.b.writeln("return r")
		g.b.endBlock()
	})
}

func (g *generator) generateGroupMap(groupIdx []groupIdx) {
	g.b.startDef("const")
	for _, e := range groupIdx {
		g.b.writeln("Group", e.key, " = ", strconv.Quote(e.key))
	}
	g.b.endDef()

	g.b.startInit("var Groups = map[string][]string")
	for _, e := range groupIdx {
		g.b.startInit("Group", e.key, ":")
		for _, decl := range e.decls {
			g.b.writeln("Cmd", decl.Name, ",")
		}
		g.b.endInit(",")
	}
	g.b.endInit()
}

func (g *generator) generateMethodConsts() {
	g.b.startDef("const")
	g.s.LoopFunc(func(decl *ast.FuncDecl) {
		g.b.writeln("Cmd", decl.Name, " = ", strconv.Quote(decl.Name))
	})
	g.b.endDef()

	g.b.startInit("var CommandNames = []string")
	g.s.LoopFunc(func(decl *ast.FuncDecl) {
		g.b.writeln("Cmd", decl.Name, ",")
	})
	g.b.endInit()
}

type groupIdx struct {
	key   string
	decls []*ast.FuncDecl
}

func (g *generator) buildGroupIdx() []groupIdx {

	groups := map[string][]*ast.FuncDecl{}

	g.s.LoopFunc(func(decl *ast.FuncDecl) {
		attr := g.s.LookupFuncAttr(decl.Attr)
		if attr == nil {
			panic("function attributes (group) not found: " + decl.Attr) // should never happen
		}

		group := stringutils.PascalCase(attr.Group)
		if _, ok := groups[group]; !ok {
			groups[group] = make([]*ast.FuncDecl, 0)
		}
		groups[group] = append(groups[group], decl)
	})

	// sort by group
	idx := make([]groupIdx, 0, len(groups))
	for group, decls := range groups {
		idx = append(idx, groupIdx{group, decls})
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i].key < idx[j].key })

	return idx
}

const header = `// Code generated by %s; DO NOT EDIT.

package %s

`

func (g *generator) generate(pkg string) ([]byte, error) {
	g.b.b.Reset()
	cmd := "rediser"
	if len(os.Args[1:]) != 0 {
		cmd = strings.Join([]string{cmd, strings.Join(os.Args[1:], " ")}, " ")
	}
	g.b.write(fmt.Sprintf(header, strconv.Quote(cmd), pkg))
	groupIdx := g.buildGroupIdx()
	g.generateEnums()
	g.generateStructs()
	g.generateInterfaces(groupIdx)
	g.generateMethods()
	g.generateGroupMap(groupIdx)
	g.generateMethodConsts()
	return g.b.b.Bytes(), nil
}
