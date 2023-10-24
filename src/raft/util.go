package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.SetPrefix("[DEBUG]")
		log.Printf(format, a...)
	}
	return
}
