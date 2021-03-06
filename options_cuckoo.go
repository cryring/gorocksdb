package gorocksdb

// #include "rocksdb/c.h"
import "C"

// CuckooOptions ...
type CuckooOptions struct {
	c *C.rocksdb_cuckoo_table_options_t
}

// NewDefaultCockooOptions creates a default cuckoo options object.
func NewDefaultCockooOptions() *CuckooOptions {
	return &CuckooOptions{C.rocksdb_cuckoo_options_create()}
}

// SetHashRatio ...
// Determines the utilization of hash tables. Smaller values
// result in larger hash tables with fewer collisions.
func (opts *CuckooOptions) SetHashRatio(value float64) {
	C.rocksdb_cuckoo_options_set_hash_ratio(opts.c, C.double(value))
}

// SetMaxSearchDepth ...
// A property used by builder to determine the depth to go to
// to search for a path to displace elements in case of
// collision. See Builder.MakeSpaceForKey method. Higher
// values result in more efficient hash tables with fewer
// lookups but take more time to build.
func (opts *CuckooOptions) SetMaxSearchDepth(value uint32) {
	C.rocksdb_cuckoo_options_set_max_search_depth(opts.c, C.uint32_t(value))
}

// SetCuckooBlockSize ...
// in case of collision while inserting, the builder
// attempts to insert in the next cuckoo_block_size
// locations before skipping over to the next Cuckoo hash
// function. This makes lookups more cache friendly in case
// of collisions.
func (opts *CuckooOptions) SetCuckooBlockSize(value uint32) {
	C.rocksdb_cuckoo_options_set_cuckoo_block_size(opts.c, C.uint32_t(value))
}

// SetIdentityAsFirstHash ...
// If this option is enabled, user key is treated as uint64_t and its value
// is used as hash value directly. This option changes builder's behavior.
// Reader ignore this option and behave according to what specified in table
// property.
func (opts *CuckooOptions) SetIdentityAsFirstHash(value bool) {
	C.rocksdb_cuckoo_options_set_identity_as_first_hash(opts.c, boolToChar(value))
}

// SetUseModuleHash ...
// If this option is set to true, module is used during hash calculation.
// This often yields better space efficiency at the cost of performance.
// If this option is set to false, # of entries in table is constrained to be
// power of two, and bit and is used to calculate hash, which is faster in
// general.
func (opts *CuckooOptions) SetUseModuleHash(value bool) {
	C.rocksdb_cuckoo_options_set_use_module_hash(opts.c, boolToChar(value))
}

// Destroy is used to free the options object.
func (opts *CuckooOptions) Destroy() {
	C.rocksdb_cuckoo_options_destroy(opts.c)
	opts.c = nil
}
