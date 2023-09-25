package mr

import (
	"log"
	"os"
)

func closeFile(file *os.File) {
	if file == nil {
		return
	}
	err := file.Close()
	if err != nil {
		log.Fatalf("cannot close file: %s", file.Name())
	}
}
