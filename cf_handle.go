package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import "unsafe"

// ColumnFamilyHandle represents a handle to a ColumnFamily.
type ColumnFamilyHandle struct {
	c    *C.rocksdb_column_family_handle_t
	name string
}

// NewNativeColumnFamilyHandle creates a ColumnFamilyHandle object.
func NewNativeColumnFamilyHandle(c *C.rocksdb_column_family_handle_t, name string) *ColumnFamilyHandle {
	return &ColumnFamilyHandle{c: c, name: name}
}

// Name returns the name of column family.
func (h *ColumnFamilyHandle) Name() string {
	return h.name
}

// UnsafeGetCFHandler returns the underlying c column family handle.
func (h *ColumnFamilyHandle) UnsafeGetCFHandler() unsafe.Pointer {
	return unsafe.Pointer(h.c)
}

// Destroy calls the destructor of the underlying column family handle.
func (h *ColumnFamilyHandle) Destroy() {
	C.rocksdb_column_family_handle_destroy(h.c)
	h.c = nil
}

// ColumnFamilyHandles represents the array of column families.
type ColumnFamilyHandles []*ColumnFamilyHandle

func (cfs ColumnFamilyHandles) toCSlice() columnFamilySlice {
	cCFs := make(columnFamilySlice, len(cfs))
	for i, cf := range cfs {
		cCFs[i] = cf.c
	}
	return cCFs
}

// ColumnFamilyDescriptor represents the descriptor of a column family.
type ColumnFamilyDescriptor struct {
	Name    string
	Options *Options
}
