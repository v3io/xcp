package main

import (
	"flag"
	"fmt"
	"github.com/v3io/xcp/backends"
	"github.com/v3io/xcp/common"
	"github.com/v3io/xcp/copydir"
)

func main() {

	recursive := flag.Bool("r", false, "Recursive (go over child dirs)")
	hidden := flag.Bool("hidden", false, "include hidden files (start with '.')")
	maxSize := flag.Int("m", 0, "maximum file size")
	minSize := flag.Int("n", 0, "minimum file size")
	workers := flag.Int("w", 1, "num of worker routines")
	filter := flag.String("f", "", "filter string e.g. *.png")
	logLevel := flag.String("v", "debug", "log level: info | debug")
	mtime := flag.String("t", "", "minimal file time e.g. 'now-7d' or RFC3339 date")
	flag.Parse()

	logger, _ := common.NewLogger(*logLevel)
	args := flag.Args()
	if len(args) != 2 {
		panic(fmt.Errorf("Error missing source or destination: usage [flags] source dest"))
	}

	src, err := common.UrlParse(args[0])
	if err != nil {
		panic(err)
	}
	dst, err := common.UrlParse(args[1])
	if err != nil {
		panic(err)
	}
	since, err := common.String2Time(*mtime)
	if err != nil {
		panic(err)
	}

	task := backends.CopyTask{
		Source:    src,
		Target:    dst,
		Since:     since,
		Filter:    *filter,
		Recursive: *recursive,
		MaxSize:   int64(*maxSize),
		MinSize:   int64(*minSize),
		Hidden:    *hidden,
	}

	if err := copydir.RunTask(&task, logger, *workers); err != nil {
		panic(err)
	}
}
