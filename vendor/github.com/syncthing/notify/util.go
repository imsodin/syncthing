// Copyright (c) 2014-2015 The Notify Authors. All rights reserved.
// Use of this source code is governed by the MIT license that can be
// found in the LICENSE file.

package notify

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const all = ^Event(0)
const sep = string(os.PathSeparator)

var errDepth = errors.New("exceeded allowed iteration count (circular symlink?)")

func min(i, j int) int {
	if i > j {
		return j
	}
	return i
}

func max(i, j int) int {
	if i < j {
		return j
	}
	return i
}

// must panics if err is non-nil.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// nonil gives first non-nil error from the given arguments.
func nonil(err ...error) error {
	for _, err := range err {
		if err != nil {
			return err
		}
	}
	return nil
}

func cleanpath(path string) (realpath string, isrec bool, err error) {
	fmt.Println("[NOTIFYDEBUG] cleanpath: start:", path)
	if strings.HasSuffix(path, "...") {
		isrec = true
		path = path[:len(path)-3]
	}
	if path, err = filepath.Abs(path); err != nil {
		return "", false, err
	}
	fmt.Println("[NOTIFYDEBUG] cleanpath: ...-removal and abs:", path)
	if path, err = canonical(path); err != nil {
		return "", false, err
	}
	return path, isrec, nil
}

// canonical resolves any symlink in the given path and returns it in a clean form.
// It expects the path to be absolute. It fails to resolve circular symlinks by
// maintaining a simple iteration limit.
func canonical(p string) (string, error) {
	p, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	fmt.Println("[NOTIFYDEBUG] canonical: start:", p)
	for i, j, depth := 1, 0, 1; i < len(p); i, depth = i+1, depth+1 {
		if depth > 128 {
			return "", &os.PathError{Op: "canonical", Path: p, Err: errDepth}
		}
		if j = strings.IndexRune(p[i:], '/'); j == -1 {
			j, i = i, len(p)
		} else {
			j, i = i, i+j
		}
		fmt.Printf("[NOTIFYDEBUG] canonical (i=%v, j=%v, depth=%v, p=%v)\n", i, j, depth, p)
		fi, err := os.Lstat(p[:i])
		if err != nil {
			fmt.Printf("[NOTIFYDEBUG] canonical: lstat err: %v\n", err)
			return "", err
		}
		if fi.Mode()&os.ModeSymlink == os.ModeSymlink {
			fmt.Printf("[NOTIFYDEBUG] canonical: symlink detected: %v\n", p[:i])
			s, err := os.Readlink(p[:i])
			if err != nil {
				fmt.Printf("[NOTIFYDEBUG] canonical: os.Readlink failed: %v\n", err)
				return "", err
			}
			fmt.Printf("[NOTIFYDEBUG] canonical: dereferenced link: %v\n", s)
			if filepath.IsAbs(s) {
				p = "/" + s + p[i:]
			} else {
				p = p[:j] + s + p[i:]
			}
			fmt.Printf("[NOTIFYDEBUG] canonical: new path: %v\n", p)
			i = 1 // no guarantee s is canonical, start all over
		}
	}
	return filepath.Clean(p), nil
}

func joinevents(events []Event) (e Event) {
	if len(events) == 0 {
		e = All
	} else {
		for _, event := range events {
			e |= event
		}
	}
	return
}

func split(s string) (string, string) {
	if i := lastIndexSep(s); i != -1 {
		return s[:i], s[i+1:]
	}
	return "", s
}

func base(s string) string {
	if i := lastIndexSep(s); i != -1 {
		return s[i+1:]
	}
	return s
}

func indexbase(root, name string) int {
	if n, m := len(root), len(name); m >= n && name[:n] == root &&
		(n == m || name[n] == os.PathSeparator) {
		return min(n+1, m)
	}
	return -1
}

func indexSep(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == os.PathSeparator {
			return i
		}
	}
	return -1
}

func lastIndexSep(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == os.PathSeparator {
			return i
		}
	}
	return -1
}
