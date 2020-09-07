// Copyright (C) 2020 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package backend

import (
	"io/ioutil"
	"testing"
)

func TestBoltBackendBehavior(t *testing.T) {
	open := func() Backend {
		fd, err := ioutil.TempFile("", "syncthing")
		if err != nil {
			t.Fatal(err)
		}
		fd.Close()
		backend, err := OpenBolt(fd.Name())
		if err != nil {
			t.Fatal(err)
		}
		return backend
	}
	testBackendBehavior(t, open)
}
