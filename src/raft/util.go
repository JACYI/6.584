package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.SetPrefix("[DEBUG]")
		log.Printf(format, a...)
	}
	return
}
