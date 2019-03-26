package gorocksdb

// #include "rocksdb/c.h"
import "C"

// PerfContext ...
type PerfContext struct {
	c *C.rocksdb_perfcontext_t
}

// NewPerfContext create a PerfContext object.
func NewPerfContext() *PerfContext {
	return &PerfContext{C.rocksdb_perfcontext_create()}
}

// Reset ...
func (pc *PerfContext) Reset() {
	C.rocksdb_perfcontext_reset(pc.c)
}

// Report ...
func (pc *PerfContext) Report(excludeZeroCounters bool) string {
	return C.GoString(C.rocksdb_perfcontext_report(pc.c, boolToChar(excludeZeroCounters)))
}

// Metric ...
func (pc *PerfContext) Metric(metric int) {
	C.rocksdb_perfcontext_metric(pc.c, C.int(metric))
}

// Destory ...
func (pc *PerfContext) Destroy() {
	C.rocksdb_perfcontext_destroy(pc.c)
}
