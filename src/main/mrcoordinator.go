package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"6.5840/mr"
	"path/filepath"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
	files := []string{}
	for _, s := range os.Args[1:] {
		fs, err := filepath.Glob(s)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: unformatted input pattern: %s\n", s)
			os.Exit(1)
		}
		files = append(files, fs...)
	}

	m := mr.MakeCoordinator(files, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
