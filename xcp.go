/*
Copyright 2019 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
package main

import (
	"flag"
	"fmt"
	"github.com/v3io/xcp/backends"
	"github.com/v3io/xcp/common"
	"github.com/v3io/xcp/operators"
	"os"
)

func main() {

	recursive := flag.Bool("r", false, "Recursive (go over child dirs)")
	hidden := flag.Bool("hidden", false, "include hidden files (start with '.')")
	copyEmpty := flag.Bool("empty", false, "include empty files (size=0), ignored by default")
	maxSize := flag.Int("m", 0, "maximum file size")
	minSize := flag.Int("n", 0, "minimum file size")
	workers := flag.Int("w", 8, "num of worker routines")
	logLevel := flag.String("v", "info", "log level: info | debug")
	mtime := flag.String("t", "", "minimal file time e.g. 'now-7d' or RFC3339 date")
	flag.Parse()

	logger, _ := common.NewLogger(*logLevel)
	args := flag.Args()
	if len(args) != 2 {
		fmt.Println("Error missing source or destination: usage xcp [flags] source dest\n")
		flag.Usage()
		os.Exit(1)
	}

	src, err := common.UrlParse(args[0], true)
	if err != nil {
		panic(err)
	}
	dst, err := common.UrlParse(args[1], true)
	if err != nil {
		panic(err)
	}
	since, err := common.String2Time(*mtime)
	if err != nil {
		panic(err)
	}

	listTask := backends.ListDirTask{
		Source:    src,
		Since:     since,
		Recursive: *recursive,
		MaxSize:   int64(*maxSize),
		MinSize:   int64(*minSize),
		Hidden:    *hidden,
		InclEmpty: *copyEmpty,
	}

	if err := operators.CopyDir(&listTask, dst, logger, *workers); err != nil {
		panic(err)
	}
}
