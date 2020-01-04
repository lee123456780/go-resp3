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
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
)

/*
optimizer analyses generated source code and optimize it to keep code generation simple.

combine consecutive adds:
	r.add(elem1)
	r.add(elem2)
	-->
	r.add(elem1, elem2)

*/

type optimizer struct {
}

func newOptimizer() *optimizer {
	return &optimizer{}
}

func (o *optimizer) isEncode(exprStmt *ast.ExprStmt) bool {
	// 27395  .  .  .  .  .  3: *ast.ExprStmt {
	// 27396  .  .  .  .  .  .  X: *ast.CallExpr {
	// 27397  .  .  .  .  .  .  .  Fun: *ast.SelectorExpr {
	// 27398  .  .  .  .  .  .  .  .  X: *ast.Ident {
	// 27399  .  .  .  .  .  .  .  .  .  NamePos: client_redis.go:780:2
	// 27400  .  .  .  .  .  .  .  .  .  Name: "c"
	// 27401  .  .  .  .  .  .  .  .  .  Obj: *(obj @ 27190)
	// 27402  .  .  .  .  .  .  .  .  }
	// 27403  .  .  .  .  .  .  .  .  Sel: *ast.Ident {
	// 27404  .  .  .  .  .  .  .  .  .  NamePos: client_redis.go:780:4
	// 27405  .  .  .  .  .  .  .  .  .  Name: "encode"
	// 27406  .  .  .  .  .  .  .  .  }
	// 27407  .  .  .  .  .  .  .  }
	// 27408  .  .  .  .  .  .  .  Lparen: client_redis.go:780:10
	// 27409  .  .  .  .  .  .  .  Args: []ast.Expr (len = 1) {
	// 27410  .  .  .  .  .  .  .  .  0: *ast.BasicLit {
	// 27411  .  .  .  .  .  .  .  .  .  ValuePos: client_redis.go:780:11
	// 27412  .  .  .  .  .  .  .  .  .  Kind: STRING
	// 27413  .  .  .  .  .  .  .  .  .  Value: "\"BITOP\""
	// 27414  .  .  .  .  .  .  .  .  }
	// 27415  .  .  .  .  .  .  .  }
	// 27416  .  .  .  .  .  .  .  Ellipsis: -
	// 27417  .  .  .  .  .  .  .  Rparen: client_redis.go:780:18
	// 27418  .  .  .  .  .  .  }
	// 27419  .  .  .  .  .  }

	callExpr, ok := exprStmt.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	selectorExpr, ok := callExpr.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := selectorExpr.X.(*ast.Ident)
	if !ok {
		return false
	}
	if ident.Name != "c" {
		return false
	}
	if selectorExpr.Sel.Name != "encode" {
		return false
	}
	return true
}

func (o *optimizer) mergeEncode(exprStmt1, exprStmt2 *ast.ExprStmt) {
	callExpr1 := exprStmt1.X.(*ast.CallExpr)
	callExpr2 := exprStmt2.X.(*ast.CallExpr)

	for i := 0; i < len(callExpr2.Args); i++ {
		callExpr1.Args = append(callExpr1.Args, callExpr2.Args[i])
	}
}

func (o *optimizer) optimize(src []byte) ([]byte, error) {

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		return src, err
	}

	ast.Inspect(f, func(node ast.Node) bool {

		var stmts *[]ast.Stmt

		switch node := node.(type) {

		case *ast.BlockStmt:
			stmts = &node.List
			node.Lbrace = token.NoPos
			node.Rbrace = token.NoPos
		case *ast.CaseClause:
			stmts = &node.Body
			node.Case = token.NoPos

		case *ast.BasicLit:
			node.ValuePos = token.NoPos
		case *ast.Ident:
			node.NamePos = token.NoPos
		case *ast.IfStmt:
			node.If = token.NoPos
		case *ast.RangeStmt:
			node.For = token.NoPos
		}

		if stmts != nil && *stmts != nil {

			var lastEncode *ast.ExprStmt
			j := 0

			for _, stmt := range *stmts {
				if exprStmt, ok := stmt.(*ast.ExprStmt); ok && o.isEncode(exprStmt) {
					if lastEncode == nil {
						lastEncode = exprStmt
						(*stmts)[j] = exprStmt
						j++
					} else {
						o.mergeEncode(lastEncode, exprStmt)
					}
				} else {
					lastEncode = nil
					(*stmts)[j] = stmt
					j++
				}
			}
			*stmts = (*stmts)[:j]
		}
		return true
	})

	var b bytes.Buffer
	if err := (&printer.Config{Tabwidth: 8}).Fprint(&b, fset, f); err != nil {
		return src, err
	}

	src, err = format.Source(b.Bytes())
	if err != nil {
		return b.Bytes(), err
	}
	return src, nil
}
