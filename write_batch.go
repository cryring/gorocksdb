package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"io"
	"unsafe"
)

// WriteBatch is a batching of Puts, Merges and Deletes.
type WriteBatch struct {
	c *C.rocksdb_writebatch_t
}

// NewWriteBatch create a WriteBatch object.
func NewWriteBatch() *WriteBatch {
	return NewNativeWriteBatch(C.rocksdb_writebatch_create())
}

// NewNativeWriteBatch create a WriteBatch object.
func NewNativeWriteBatch(c *C.rocksdb_writebatch_t) *WriteBatch {
	return &WriteBatch{c}
}

// WriteBatchFrom creates a write batch from a serialized WriteBatch.
func WriteBatchFrom(data []byte) *WriteBatch {
	return NewNativeWriteBatch(C.rocksdb_writebatch_create_from(byteToChar(data), C.size_t(len(data))))
}

// Put queues a key-value pair.
func (wb *WriteBatch) Put(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutV queues a key(SliceParts)-value(SliceParts) pair.
func (wb *WriteBatch) PutV(keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_putv(
		wb.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// PutCF queues a key-value pair in a column family.
func (wb *WriteBatch) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_put_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutVCF queues a key(SliceParts)-value(SliceParts) pair in a column family.
func (wb *WriteBatch) PutVCF(cf *ColumnFamilyHandle, keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_putv_cf(
		wb.c,
		cf.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// Merge queues a merge of "value" with the existing value of "key".
func (wb *WriteBatch) Merge(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeV queues a merge of "value" with the existing value of "key".
func (wb *WriteBatch) MergeV(keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_mergev(
		wb.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// MergeCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (wb *WriteBatch) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_merge_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeVCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (wb *WriteBatch) MergeVCF(cf *ColumnFamilyHandle, keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_mergev_cf(
		wb.c,
		cf.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// Delete queues a deletion of the data at key.
func (wb *WriteBatch) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete(wb.c, cKey, C.size_t(len(key)))
}

// DeleteV queues a deletion of the data at key.
func (wb *WriteBatch) DeleteV(keys [][]byte) {
	cKeys, cKeysSize := byteSlicesToCSlices(keys)
	defer cKeys.Destroy()

	C.rocksdb_writebatch_deletev(
		wb.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
	)
}

// DeleteRange queues a deletion range of the data at key.
func (wb *WriteBatch) DeleteRange(begin, end []byte) {
	var (
		cBegin = byteToChar(begin)
		cEnd   = byteToChar(end)
	)
	C.rocksdb_writebatch_delete_range(wb.c, cBegin, C.size_t(len(begin)), cEnd, C.size_t(len(end)))
}

// DeleteCF queues a deletion of the data at key in a column family.
func (wb *WriteBatch) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_delete_cf(wb.c, cf.c, cKey, C.size_t(len(key)))
}

// DeleteVCF queues a deletion of the data at key.
func (wb *WriteBatch) DeleteVCF(cf *ColumnFamilyHandle, keys [][]byte) {
	cKeys, cKeysSize := byteSlicesToCSlices(keys)
	defer cKeys.Destroy()

	C.rocksdb_writebatch_deletev_cf(
		wb.c,
		cf.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
	)
}

// DeleteRangeCF queues a deletion range of the data at key in a column family.
func (wb *WriteBatch) DeleteRangeCF(cf *ColumnFamilyHandle, begin, end []byte) {
	var (
		cBegin = byteToChar(begin)
		cEnd   = byteToChar(end)
	)
	C.rocksdb_writebatch_delete_range_cf(wb.c, cf.c, cBegin, C.size_t(len(begin)), cEnd, C.size_t(len(end)))
}

// PutLogData ...
func (wb *WriteBatch) PutLogData(blob []byte) {
	cBlob := byteToChar(blob)
	C.rocksdb_writebatch_put_log_data(wb.c, cBlob, C.size_t(len(blob)))
}

// Data returns the serialized version of this batch.
func (wb *WriteBatch) Data() []byte {
	var cSize C.size_t
	cValue := C.rocksdb_writebatch_data(wb.c, &cSize)
	return charToByte(cValue, cSize)
}

// SetSavePoint records the state of the batch for future calls to RollbackToSavePoint().
// May be called multiple times to set multiple save points.
func (wb *WriteBatch) SetSavePoint() {
	C.rocksdb_writebatch_set_save_point(wb.c)
}

// RollBackToSavePoint remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
// most recent call to SetSavePoint() and removes the most recent save point.
// If there is no previous call to SetSavePoint(), Status::NotFound()
// will be returned.
// Otherwise returns Status::OK().
func (wb *WriteBatch) RollBackToSavePoint() error {
	var cErr *C.char
	C.rocksdb_writebatch_rollback_to_save_point(wb.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Count returns the number of updates in the batch.
func (wb *WriteBatch) Count() int {
	return int(C.rocksdb_writebatch_count(wb.c))
}

// NewIterator returns a iterator to iterate over the records in the batch.
func (wb *WriteBatch) NewIterator() *WriteBatchIterator {
	data := wb.Data()
	if len(data) < 8+4 {
		return &WriteBatchIterator{}
	}
	return &WriteBatchIterator{data: data[12:]}
}

// Clear removes all the enqueued Put and Deletes.
func (wb *WriteBatch) Clear() {
	C.rocksdb_writebatch_clear(wb.c)
}

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatch) Destroy() {
	C.rocksdb_writebatch_destroy(wb.c)
	wb.c = nil
}

// WriteBatchRecordType describes the type of a batch record.
type WriteBatchRecordType byte

// Types of batch records.
const (
	WriteBatchDeletionRecord                 WriteBatchRecordType = 0x0
	WriteBatchValueRecord                    WriteBatchRecordType = 0x1
	WriteBatchMergeRecord                    WriteBatchRecordType = 0x2
	WriteBatchLogDataRecord                  WriteBatchRecordType = 0x3
	WriteBatchCFDeletionRecord               WriteBatchRecordType = 0x4
	WriteBatchCFValueRecord                  WriteBatchRecordType = 0x5
	WriteBatchCFMergeRecord                  WriteBatchRecordType = 0x6
	WriteBatchSingleDeletionRecord           WriteBatchRecordType = 0x7
	WriteBatchCFSingleDeletionRecord         WriteBatchRecordType = 0x8
	WriteBatchBeginPrepareXIDRecord          WriteBatchRecordType = 0x9
	WriteBatchEndPrepareXIDRecord            WriteBatchRecordType = 0xA
	WriteBatchCommitXIDRecord                WriteBatchRecordType = 0xB
	WriteBatchRollbackXIDRecord              WriteBatchRecordType = 0xC
	WriteBatchNoopRecord                     WriteBatchRecordType = 0xD
	WriteBatchRangeDeletion                  WriteBatchRecordType = 0xF
	WriteBatchCFRangeDeletion                WriteBatchRecordType = 0xE
	WriteBatchCFBlobIndex                    WriteBatchRecordType = 0x10
	WriteBatchBlobIndex                      WriteBatchRecordType = 0x11
	WriteBatchBeginPersistedPrepareXIDRecord WriteBatchRecordType = 0x12
	WriteBatchNotUsedRecord                  WriteBatchRecordType = 0x7F
)

// WriteBatchRecord represents a record inside a WriteBatch.
type WriteBatchRecord struct {
	CF    int
	Key   []byte
	Value []byte
	Type  WriteBatchRecordType
}

// WriteBatchIterator represents a iterator to iterator over records.
type WriteBatchIterator struct {
	data   []byte
	record WriteBatchRecord
	err    error
}

// Next returns the next record.
// Returns false if no further record exists.
func (iter *WriteBatchIterator) Next() bool {
	if iter.err != nil || len(iter.data) == 0 {
		return false
	}
	// reset the current record
	iter.record.CF = 0
	iter.record.Key = nil
	iter.record.Value = nil

	// parse the record type
	iter.record.Type = iter.decodeRecType()

	switch iter.record.Type {
	case
		WriteBatchDeletionRecord,
		WriteBatchSingleDeletionRecord:
		iter.record.Key = iter.decodeSlice()
	case
		WriteBatchCFDeletionRecord,
		WriteBatchCFSingleDeletionRecord:
		iter.record.CF = int(iter.decodeVarint())
		if iter.err == nil {
			iter.record.Key = iter.decodeSlice()
		}
	case
		WriteBatchValueRecord,
		WriteBatchMergeRecord,
		WriteBatchRangeDeletion,
		WriteBatchBlobIndex:
		iter.record.Key = iter.decodeSlice()
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case
		WriteBatchCFValueRecord,
		WriteBatchCFRangeDeletion,
		WriteBatchCFMergeRecord,
		WriteBatchCFBlobIndex:
		iter.record.CF = int(iter.decodeVarint())
		if iter.err == nil {
			iter.record.Key = iter.decodeSlice()
		}
		if iter.err == nil {
			iter.record.Value = iter.decodeSlice()
		}
	case WriteBatchLogDataRecord:
		iter.record.Value = iter.decodeSlice()
	case
		WriteBatchNoopRecord,
		WriteBatchBeginPrepareXIDRecord,
		WriteBatchBeginPersistedPrepareXIDRecord:
	case
		WriteBatchEndPrepareXIDRecord,
		WriteBatchCommitXIDRecord,
		WriteBatchRollbackXIDRecord:
		iter.record.Value = iter.decodeSlice()
	default:
		iter.err = errors.New("unsupported wal record type")
	}

	return iter.err == nil

}

// Record returns the current record.
func (iter *WriteBatchIterator) Record() *WriteBatchRecord {
	return &iter.record
}

// Error returns the error if the iteration is failed.
func (iter *WriteBatchIterator) Error() error {
	return iter.err
}

func (iter *WriteBatchIterator) decodeSlice() []byte {
	l := int(iter.decodeVarint())
	if l > len(iter.data) {
		iter.err = io.ErrShortBuffer
	}
	if iter.err != nil {
		return []byte{}
	}
	ret := iter.data[:l]
	iter.data = iter.data[l:]
	return ret
}

func (iter *WriteBatchIterator) decodeRecType() WriteBatchRecordType {
	if len(iter.data) == 0 {
		iter.err = io.ErrShortBuffer
		return WriteBatchNotUsedRecord
	}
	t := iter.data[0]
	iter.data = iter.data[1:]
	return WriteBatchRecordType(t)
}

func (iter *WriteBatchIterator) decodeVarint() uint64 {
	var n int
	var x uint64
	for shift := uint(0); shift < 64 && n < len(iter.data); shift += 7 {
		b := uint64(iter.data[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			iter.data = iter.data[n:]
			return x
		}
	}
	if n == len(iter.data) {
		iter.err = io.ErrShortBuffer
	} else {
		iter.err = errors.New("malformed varint")
	}
	return 0
}

// WriteBatchWithIndex with a binary searchable index built for all the keys inserted.
type WriteBatchWithIndex struct {
	c *C.rocksdb_writebatch_wi_t
}

// NewWriteBatchWithIndex create a WriteBatchWithIndex object.
func NewWriteBatchWithIndex(reservedBytes int, overwriteKey byte) *WriteBatchWithIndex {
	return &WriteBatchWithIndex{C.rocksdb_writebatch_wi_create(C.size_t(reservedBytes), C.uchar(overwriteKey))}
}

// NewNativeWriteBatchWithIndex create a WriteBatchWithIndex object.
func NewNativeWriteBatchWithIndex(c *C.rocksdb_writebatch_wi_t) *WriteBatchWithIndex {
	return &WriteBatchWithIndex{c}
}

// WriteBatchWithIndexFrom creates a write batch from a serialized WriteBatch.
// func WriteBatchWithIndexFrom(data []byte) *WriteBatchWithIndex {
// 	return NewNativeWriteBatchWithIndex(C.rocksdb_writebatch_wi_create_from(byteToChar(data), C.size_t(len(data))))
// }

// Put queues a key-value pair.
func (wb *WriteBatchWithIndex) Put(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_put(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutV queues a key(SliceParts)-value(SliceParts) pair.
func (wb *WriteBatchWithIndex) PutV(keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_wi_putv(
		wb.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// PutCF queues a key-value pair in a column family.
func (wb *WriteBatchWithIndex) PutCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_put_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// PutVCF queues a key(SliceParts)-value(SliceParts) pair in a column family.
func (wb *WriteBatchWithIndex) PutVCF(cf *ColumnFamilyHandle, keys, values [][]byte) {
	var (
		cKeys, cKeysSize     = byteSlicesToCSlices(keys)
		cValues, cValuesSize = byteSlicesToCSlices(values)
	)
	defer cKeys.Destroy()
	defer cValues.Destroy()

	C.rocksdb_writebatch_wi_putv_cf(
		wb.c,
		cf.c,
		C.int(len(keys)),
		cKeys.c(),
		cKeysSize.c(),
		C.int(len(values)),
		cValues.c(),
		cValuesSize.c(),
	)
}

// Merge queues a merge of "value" with the existing value of "key".
func (wb *WriteBatchWithIndex) Merge(key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_merge(wb.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// MergeCF queues a merge of "value" with the existing value of "key" in a
// column family.
func (wb *WriteBatchWithIndex) MergeCF(cf *ColumnFamilyHandle, key, value []byte) {
	cKey := byteToChar(key)
	cValue := byteToChar(value)
	C.rocksdb_writebatch_wi_merge_cf(wb.c, cf.c, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)))
}

// Delete queues a deletion of the data at key.
func (wb *WriteBatchWithIndex) Delete(key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_wi_delete(wb.c, cKey, C.size_t(len(key)))
}

// DeleteRange queues a deletion range of the data at key.
func (wb *WriteBatchWithIndex) DeleteRange(begin, end []byte) {
	var (
		cBegin = byteToChar(begin)
		cEnd   = byteToChar(end)
	)
	C.rocksdb_writebatch_wi_delete_range(wb.c, cBegin, C.size_t(len(begin)), cEnd, C.size_t(len(end)))
}

// DeleteCF queues a deletion of the data at key in a column family.
func (wb *WriteBatchWithIndex) DeleteCF(cf *ColumnFamilyHandle, key []byte) {
	cKey := byteToChar(key)
	C.rocksdb_writebatch_wi_delete_cf(wb.c, cf.c, cKey, C.size_t(len(key)))
}

// DeleteRangeCF queues a deletion range of the data at key in a column family.
func (wb *WriteBatchWithIndex) DeleteRangeCF(cf *ColumnFamilyHandle, begin, end []byte) {
	var (
		cBegin = byteToChar(begin)
		cEnd   = byteToChar(end)
	)
	C.rocksdb_writebatch_wi_delete_range_cf(wb.c, cf.c, cBegin, C.size_t(len(begin)), cEnd, C.size_t(len(end)))
}

// PutLogData ...
func (wb *WriteBatchWithIndex) PutLogData(blob []byte) {
	cBlob := byteToChar(blob)
	C.rocksdb_writebatch_wi_put_log_data(wb.c, cBlob, C.size_t(len(blob)))
}

// Data returns the serialized version of this batch.
func (wb *WriteBatchWithIndex) Data() []byte {
	var cSize C.size_t
	cValue := C.rocksdb_writebatch_wi_data(wb.c, &cSize)
	return charToByte(cValue, cSize)
}

// SetSavePoint records the state of the batch for future calls to RollbackToSavePoint().
// May be called multiple times to set multiple save points.
func (wb *WriteBatchWithIndex) SetSavePoint() {
	C.rocksdb_writebatch_wi_set_save_point(wb.c)
}

// RollBackToSavePoint remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
// most recent call to SetSavePoint() and removes the most recent save point.
// If there is no previous call to SetSavePoint(), Status::NotFound()
// will be returned.
// Otherwise returns Status::OK().
func (wb *WriteBatchWithIndex) RollBackToSavePoint() error {
	var cErr *C.char
	C.rocksdb_writebatch_wi_rollback_to_save_point(wb.c, &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}

// Count returns the number of updates in the batch.
func (wb *WriteBatchWithIndex) Count() int {
	return int(C.rocksdb_writebatch_wi_count(wb.c))
}

// Clear removes all the enqueued Put and Deletes.
func (wb *WriteBatchWithIndex) Clear() {
	C.rocksdb_writebatch_wi_clear(wb.c)
}

// Destroy deallocates the WriteBatch object.
func (wb *WriteBatchWithIndex) Destroy() {
	C.rocksdb_writebatch_wi_destroy(wb.c)
	wb.c = nil
}
