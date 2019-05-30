// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package diskoverflow

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type Common interface {
	Get(k []byte) ([]byte, bool)
	Pop(k []byte) ([]byte, bool)
	Delete(k []byte)
	NewIterator() iterator.Iterator
	Items() int
	Close()
}

type Map interface {
	Common
	Set(k, v []byte)
}

type omap struct {
	db       *leveldb.DB
	items    int
	location string
}

// NewMap returns an implementation of Map, spilling to disk at location.
func NewMap(location string, overflowBytes int) Map {
	tmp, err := ioutil.TempDir(location, "overflow-")
	errPanic(err)
	db, err := leveldb.OpenFile(tmp, &opt.Options{
		OpenFilesCacheCapacity: 10,
		WriteBuffer:            overflowBytes,
	})
	errPanic(err)
	return &omap{
		db:       db,
		location: tmp,
	}
}

func (o *omap) Set(k, v []byte) {
	_, ok := o.Get(k)
	errPanic(o.db.Put(k, v, nil))
	if !ok {
		o.items++
	}
}

func (o *omap) String() string {
	return fmt.Sprintf("Map@%p", o)
}

func (o *omap) Close() {
	o.db.Close()
	os.RemoveAll(o.location)
}

func (o *omap) Get(k []byte) ([]byte, bool) {
	v, err := o.db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}
	errPanic(err)
	return v, true
}

func (o *omap) Items() int {
	return o.items
}

func (o *omap) Pop(k []byte) ([]byte, bool) {
	v, ok := o.Get(k)
	if ok {
		o.Delete(k)
	}
	return v, ok
}

func (o *omap) Delete(k []byte) {
	errPanic(o.db.Delete(k, nil))
	o.items--
}

func (o *omap) NewIterator() iterator.Iterator {
	return o.db.NewIterator(nil, nil)
}
