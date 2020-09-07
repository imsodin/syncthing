// Copyright (C) 2020 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package backend

import (
	"bytes"
	"errors"
	"time"

	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

var onlyBucket = []byte("syncthing")

func OpenBolt(path string) (Backend, error) {
	opts := &bolt.Options{
		Timeout: time.Second,
	}
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(onlyBucket)
		return err
	})
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	return &boltBackend{db: db}, nil
}

type boltBackend struct {
	db      *bolt.DB
	closeWG *closeWaitGroup
}

func (b *boltBackend) NewReadTransaction() (ReadTransaction, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return newBoltReadTransaction(tx), nil
}

func (b *boltBackend) NewWriteTransaction(hooks ...CommitHook) (WriteTransaction, error) {
	tx, err := b.db.Begin(true)
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	return newBoltWriteTransaction(tx, hooks), nil
}

func (b *boltBackend) Get(key []byte) ([]byte, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	defer tx.Rollback()
	return withNotExists(tx.Bucket(onlyBucket).Get(key))
}

func (b *boltBackend) NewPrefixIterator(prefix []byte) (Iterator, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	c := tx.Cursor()
	it := &boltIterator{
		c:      c,
		tx:     tx,
		prefix: prefix,
	}
	return it, nil
}

func (b *boltBackend) NewRangeIterator(first, last []byte) (Iterator, error) {
	tx, err := b.db.Begin(false)
	if err != nil {
		return nil, wrapBoltErr(err)
	}
	c := tx.Cursor()
	it := &boltIterator{
		c:     c,
		tx:    tx,
		first: first,
		last:  last,
	}
	return it, nil
}

func (b *boltBackend) Put(key, val []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return wrapBoltErr(err)
	}
	if err := tx.Bucket(onlyBucket).Put(key, val); err != nil {
		return wrapBoltErr(err)
	}
	return wrapBoltErr(tx.Commit())
}

func (b *boltBackend) Delete(key []byte) error {
	tx, err := b.db.Begin(true)
	if err != nil {
		return wrapBoltErr(err)
	}
	if err := tx.Bucket(onlyBucket).Delete(key); err != nil {
		return wrapBoltErr(err)
	}
	return wrapBoltErr(tx.Commit())
}

func (b *boltBackend) Close() error {
	return b.db.Close()
}

func (b *boltBackend) Compact() error {
	return nil
}

type boltReadTransaction struct {
	tx  *bolt.Tx
	bkt *bolt.Bucket
}

func newBoltReadTransaction(tx *bolt.Tx) *boltReadTransaction {
	return &boltReadTransaction{
		tx:  tx,
		bkt: tx.Bucket(onlyBucket),
	}
}

func (ro *boltReadTransaction) Get(key []byte) ([]byte, error) {
	return withNotExists(ro.bkt.Get(key))
}

func (ro *boltReadTransaction) NewPrefixIterator(prefix []byte) (Iterator, error) {
	c := ro.tx.Cursor()
	it := &boltIterator{
		c:      c,
		prefix: prefix,
	}
	return it, nil
}

func (ro *boltReadTransaction) NewRangeIterator(first, last []byte) (Iterator, error) {
	c := ro.tx.Cursor()
	it := &boltIterator{
		c:     c,
		first: first,
		last:  last,
	}
	return it, nil
}

func (ro *boltReadTransaction) Release() {
	_ = ro.tx.Rollback()
}

type boltWriteTransaction struct {
	boltReadTransaction
	hooks []CommitHook
}

func newBoltWriteTransaction(tx *bolt.Tx, hooks []CommitHook) *boltWriteTransaction {
	bkt := tx.Bucket(onlyBucket)
	return &boltWriteTransaction{
		boltReadTransaction: boltReadTransaction{
			tx:  tx,
			bkt: bkt,
		},
		hooks: hooks,
	}
}

func (rw *boltWriteTransaction) Put(key, val []byte) error {
	return wrapBoltErr(rw.bkt.Put(key, val))
}

func (rw *boltWriteTransaction) Delete(key []byte) error {
	return wrapBoltErr(rw.bkt.Delete(key))
}

func (rw *boltWriteTransaction) Checkpoint() error {
	return nil
}

func (rw *boltWriteTransaction) Commit() error {
	return wrapBoltErr(rw.tx.Commit())
}

type boltIterator struct {
	c           *bolt.Cursor
	tx          *bolt.Tx // only set if we should close the tx
	prefix      []byte
	first, last []byte
	key, val    []byte
	didSeek     bool
	done        bool
}

func (it *boltIterator) Next() bool {
	if it.done {
		return false
	}

	var k, v []byte
	if it.didSeek {
		k, v = it.c.Next()
	} else {
		if it.prefix != nil {
			k, v = it.c.Seek(it.prefix)
		} else {
			k, v = it.c.Seek(it.first)
		}
		it.didSeek = true
	}
	if k == nil {
		// Iterator reached end
		it.done = true
		return false
	}
	if it.prefix != nil && !bytes.HasPrefix(k, it.prefix) {
		// Iterator passed outside prefix
		it.done = true
		return false
	}
	if it.last != nil && bytes.Compare(k, it.last) > 0 {
		// Iterator passed the `last` key
		it.done = true
		return false
	}
	it.key = k
	it.val = v
	return true
}

func (it *boltIterator) Key() []byte {
	return it.key
}

func (it *boltIterator) Value() []byte {
	return it.val
}

func (it *boltIterator) Error() error {
	// XXX: Cursors are immune to errors...?
	return nil
}

func (it *boltIterator) Release() {
	// Cursor doesn't need releasing. We might have created a transaction
	// just for the iterator, and then we should close that.
	it.tx.Rollback()
	it.tx = nil
	it.done = true
}

func withNotExists(data []byte) ([]byte, error) {
	if data == nil {
		return nil, &errNotFound{}
	}
	return data, nil
}

// wrapBoltErr wraps errors so that the backend package can recognize them
func wrapBoltErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, bbolt.ErrDatabaseNotOpen) {
		return &errClosed{}
	}
	if errors.Is(err, bbolt.ErrTxClosed) {
		return &errClosed{}
	}
	return err
}
