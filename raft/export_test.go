package raft

// LogRotateBytesPtr exposes logRotateBytes so tests can override it
var LogRotateBytesPtr = &logRotateBytes
