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
	"encoding/json"
	"flag"
	goAst "go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go-resp3/cmd/rediser/internal/ast"
)

const (
	currentDir    = "."
	redisDocDir   = "../redis-doc"
	redisCommands = "commands.json"
	patchDir      = "../cmd/rediser"
	patchJSON     = "patch.json"
)

const (
	envGOARCH    = "GOARCH"
	envGOOS      = "GOOS"
	envGOFILE    = "GOFILE"
	envGOLINE    = "GOLINE"
	envGOPACKAGE = "GOPACKAGE"
	envDOLLAR    = "DOLLAR"
)

var defaultEnvValue = map[envVar]string{
	envGOFILE:    "main",
	envGOPACKAGE: "main",
}

type envVar string

func (e envVar) value() string {
	if v, ok := os.LookupEnv(string(e)); ok || v != "" {
		return v
	}
	return defaultEnvValue[e]
}

var goPackage envVar = envGOPACKAGE
var goFile envVar = envGOFILE

func outputFile() string {
	file := goFile.value()
	ext := filepath.Ext(file)
	return filepath.Join(currentDir, strings.TrimSuffix(file, ext)+"_redis.go")
}

func jsonOutputFile() string {
	file := goFile.value()
	ext := filepath.Ext(file)
	return filepath.Join(patchDir, strings.TrimSuffix(file, ext)+"_redis.json")
}

func astOutputFile() string {
	file := goFile.value()
	ext := filepath.Ext(file)
	return filepath.Join(patchDir, strings.TrimSuffix(file, ext)+"_redis.ast")
}

var (
	redis      = flag.String("redis", filepath.Join(redisDocDir, redisCommands), "redis doc comand.json file")
	patch      = flag.String("patch", filepath.Join(patchDir, patchJSON), "patch ast file")
	output     = flag.String("output", outputFile(), "output file name")
	jsonOutput = flag.String("jsonOutput", jsonOutputFile(), "json output file name")
	astOutput  = flag.String("astOutput", astOutputFile(), "ast output file name")
	pkg        = flag.String("package", goPackage.value(), "package")
)

func main() {

	flag.Parse()
	if *output == "" {
		log.Fatalf("output file missing")
	}
	if *pkg == "" {
		log.Fatalf("package missing")
	}

	// command line arguments
	log.Printf("command line arguments: %s", strings.Join(os.Args, ","))

	// go generate flag values
	log.Print("flags:")
	flag.VisitAll(func(flag *flag.Flag) {
		log.Printf(" %s: %s", flag.Name, flag.Value.String())
	})

	// show progress
	var wg sync.WaitGroup
	defer wg.Wait()

	done := progress(&wg)
	wg.Add(1)
	defer close(done)

	// start
	var commands commands
	if err := readJSONFile(*redis, &commands); err != nil {
		log.Fatalf("read file %s error: %s", *redis, err)
	}

	var list ast.DeclNodeList
	if err := readJSONFile(*patch, &list); err != nil {
		log.Fatalf("read file %s error: %s", *patch, err)
	}

	s := ast.NewScope(list)
	newConverter(s).convert(commands)

	src, err := newGenerator(s).generate(*pkg)
	if err != nil {
		log.Fatalf("generation error: %s", err)
	}

	src, err = newOptimizer().optimize(src)
	if err != nil {
		log.Printf("code format error: %s", err)
	}

	if err := writeFile(*output, src); err != nil {
		log.Fatalf("write file %s error: %s", *output, err)
	}

	if *jsonOutput != "" {
		if err := writeJSONFile(*jsonOutput, s.NodeList()); err != nil {
			log.Fatalf("write file %s error: %s", *jsonOutput, err)
		}
	}

	//write ast (optimizer debugging)
	if *astOutput != "" {
		if err := writeAstFile(*astOutput, *output); err != nil {
			log.Fatalf("write file %s error: %s", *astOutput, err)
		}
	}
}

func readJSONFile(filename string, v interface{}) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSONFile(filename string, v interface{}) error {
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetIndent("", "\t")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return writeFile(filename, b.Bytes())
}

func writeAstFile(astFilename, sourceFilename string) error {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, sourceFilename, nil, 0)
	if err != nil {
		return err
	}
	file, err := os.Create(astFilename)
	if err != nil {
		return err
	}
	goAst.Fprint(file, fset, f, goAst.NotNilFilter)
	return nil
}

func writeFile(filename string, b []byte) error {
	return ioutil.WriteFile(filename, b, 0644)
}

func progress(wg *sync.WaitGroup) chan struct{} {
	done := make(chan struct{}, 0)

	go func() {
		loop := true
		for loop {
			select {
			case <-done:
				loop = false
			case <-time.After(100 * time.Millisecond):
				print(".")
			}
		}
		println()
		wg.Done()
	}()

	return done
}
