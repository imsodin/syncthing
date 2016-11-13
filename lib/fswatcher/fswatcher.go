// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package fswatcher

import (
	"errors"
	"github.com/zillode/notify"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/scanner"
	"github.com/syncthing/syncthing/lib/sync"
	"github.com/syncthing/syncthing/lib/ignore"
)

type FsEvent struct {
	path string
	time time.Time
}

var Tempnamer scanner.TempNamer

type FsEventsBatch map[string]*FsEvent

type trackedDir struct {
	count       int
	childEvents map[string]*FsEvent
}

type FsWatcher struct {
	folderPath            string
	notifyModelChan       chan<- FsEventsBatch
	fsEvents              FsEventsBatch
	fsEventChan           <-chan notify.EventInfo
	WatchingFs            bool
	notifyDelay           time.Duration
	scanDelay             time.Duration
	lastScanNotification  time.Time
	notifyTimer           *time.Timer
	notifyTimerNeedsReset bool
	inProgress            map[string]struct{}
	ignores               *ignore.Matcher
	ignoresLock           sync.RWMutex
	folderID              string
	trackedDirs           map[string]*trackedDir
}

const (
	slowNotifyDelay = time.Duration(60) * time.Second
	fastNotifyDelay = time.Duration(500) * time.Millisecond
	scanDelay       = time.Duration(1) * time.Second
	maxFiles        = 512
	maxFilesPerDir  = 128
)

func NewFsWatcher(folderPath string, ignores *ignore.Matcher, folderID string) *FsWatcher {
	return &FsWatcher{
		folderPath:            folderPath,
		notifyModelChan:       nil,
		fsEvents:              make(FsEventsBatch),
		fsEventChan:           nil,
		WatchingFs:            false,
		notifyDelay:           fastNotifyDelay,
		notifyTimerNeedsReset: false,
		inProgress:            make(map[string]struct{}),
		ignores:               ignores,
		ignoresLock:           sync.NewRWMutex(),
		folderID:              folderID,
		trackedDirs:           make(map[string]*trackedDir),
	}
}

func (watcher *FsWatcher) StartWatchingFilesystem() (<-chan FsEventsBatch, error) {
	fsEventChan, err := watcher.setupNotifications()
	if err == nil {
		watcher.WatchingFs = true
		watcher.fsEventChan = fsEventChan
		go watcher.watchFilesystem()
	}
	notifyModelChan := make(chan FsEventsBatch)
	watcher.notifyModelChan = notifyModelChan
	return notifyModelChan, err
}

func (watcher *FsWatcher) setupNotifications() (chan notify.EventInfo, error) {
	c := make(chan notify.EventInfo, maxFiles)
	if err := notify.Watch(filepath.Join(watcher.folderPath, "..."), c, notify.All); err != nil {
		notify.Stop(c)
		close(c)
		return nil, interpretNotifyWatchError(err, watcher.folderPath)
	}
	watcher.debugf("Setup filesystem notification for %s", watcher.folderPath)
	return c, nil
}

func (watcher *FsWatcher) watchFilesystem() {
	watcher.notifyTimer = time.NewTimer(watcher.notifyDelay)
	defer watcher.notifyTimer.Stop()
	inProgressItemSubscription := events.Default.Subscribe(
		events.ItemStarted | events.ItemFinished)
	for {
		watcher.resetNotifyTimerIfNeeded()
		select {
		case event, _ := <-watcher.fsEventChan:
			newEvent := watcher.newFsEvent(event.Path())
			if newEvent != nil {
				watcher.speedUpNotifyTimer()
				watcher.storeFsEvent(newEvent)
			}
		case <-watcher.notifyTimer.C:
			watcher.actOnTimer()
		case event := <-inProgressItemSubscription.C():
			watcher.updateInProgressSet(event)
		}
	}
}

func (watcher *FsWatcher) newFsEvent(eventPath string) *FsEvent {
	if len(watcher.fsEvents) < maxFiles &&
		isSubpath(eventPath, watcher.folderPath) {
		path, _ := filepath.Rel(watcher.folderPath, eventPath)
		if !watcher.shouldIgnore(path) {
			return &FsEvent{path, time.Now()}
		}
	}
	return nil
}

func isSubpath(path string, folderPath string) bool {
	if len(path) > 1 && os.IsPathSeparator(path[len(path)-1]) {
		path = path[0 : len(path)-1]
	}
	if len(folderPath) > 1 && os.IsPathSeparator(folderPath[len(folderPath)-1]) {
		folderPath = folderPath[0 : len(folderPath)-1]
	}
	return strings.HasPrefix(path, folderPath)
}

func (watcher *FsWatcher) resetNotifyTimerIfNeeded() {
	if watcher.notifyTimerNeedsReset {
		watcher.debugf("Resetting notifyTimer to %#v\n", watcher.notifyDelay)
		watcher.notifyTimer.Reset(watcher.notifyDelay)
		watcher.notifyTimerNeedsReset = false
	}
}

func (watcher *FsWatcher) speedUpNotifyTimer() {
	if watcher.notifyDelay != fastNotifyDelay {
		watcher.notifyDelay = fastNotifyDelay
		watcher.debugf("Speeding up notifyTimer to %#v\n", fastNotifyDelay)
		watcher.notifyTimerNeedsReset = true
	}
}

func (watcher *FsWatcher) slowDownNotifyTimer() {
	if watcher.notifyDelay != slowNotifyDelay {
		watcher.notifyDelay = slowNotifyDelay
		watcher.debugf("Slowing down notifyTimer to %#v\n", watcher.notifyDelay)
		watcher.notifyTimerNeedsReset = true
	}
}

func (watcher *FsWatcher) storeFsEvent(event *FsEvent) {
	if watcher.pathInProgress(event.path) {
		watcher.debugf("Skipping notification for finished path: %s\n",
			event.path)
	} else {
		watcher.fsEvents[event.path] = event
	}
}

func (watcher *FsWatcher) actOnTimer() {
	watcher.notifyTimerNeedsReset = true
	if len(watcher.fsEvents) > maxFiles {
		if time.Now().Sub(watcher.lastScanNotification) > scanDelay {
			fsEvents := make(FsEventsBatch)
			fsEvents[""] = &FsEvent{"", time.Now()}
			watcher.debugf("Too many changes, issuing full rescan.")
			watcher.notifyModelChan <- fsEvents
			watcher.fsEvents = make(FsEventsBatch)
			watcher.trackedDirs = make(map[string]*trackedDir)
		}
	} else if len(watcher.fsEvents) > 0 {
		watcher.aggregateChanges()
		if time.Now().Sub(watcher.lastScanNotification) > scanDelay {
			watcher.debugf("Notifying about %d fs events\n", len(watcher.fsEvents))
			oldFsEvents := make(FsEventsBatch)
			currTime := time.Now()
			for path, event := range watcher.fsEvents {
				if currTime.Sub(event.time) > scanDelay {
					oldFsEvents[path] = event
					delete(watcher.fsEvents, path)
					delete(watcher.trackedDirs, path)
					watcher.trackedDirs[filepath.Dir(path)].count--
				}
			}
			watcher.notifyModelChan <- oldFsEvents
		}
	} else {
		watcher.slowDownNotifyTimer()
	}
}

func (watcher *FsWatcher) aggregateChanges() {
	for path, event := range watcher.fsEvents {
		dir := filepath.Dir(path)
		if parentEvent, ok := watcher.fsEvents[dir]; ok {
			watcher.debugf("Aggregating: Parent directory of %s already tracked.", path)
			if event.time.Before(parentEvent.time) {
				parentEvent.time = event.time
			}
			delete(watcher.fsEvents, path)
		} else if watcher.trackedDirs[dir].count == maxFilesPerDir {
			watcher.debugf("Aggregating: Dir %s contains more than %d events, scan it entirely",
				dir, maxFilesPerDir)
			watcher.fsEvents[dir] = &FsEvent{dir, event.time}
			parentEvent := watcher.fsEvents[dir]
			delete(watcher.fsEvents, path)
			for childPath, childEvent := range watcher.trackedDirs[dir].childEvents {
				if childEvent.time.Before(parentEvent.time) {
					parentEvent.time = event.time
				}
				delete(watcher.fsEvents, childPath)
			}
			delete(watcher.trackedDirs, dir)
		} else {
			watcher.trackedDirs[dir].count++
			watcher.trackedDirs[dir].childEvents[path] = event
		}
	}
}

func (watcher *FsWatcher) events() []*FsEvent {
	list := make([]*FsEvent, 0, len(watcher.fsEvents))
	for _, event := range watcher.fsEvents {
		list = append(list, event)
	}
	return list
}

func (watcher *FsWatcher) updateInProgressSet(event events.Event) {
	if event.Type == events.ItemStarted {
		path := event.Data.(map[string]string)["item"]
		watcher.inProgress[path] = struct{}{}
	} else if event.Type == events.ItemFinished {
		path := event.Data.(map[string]interface{})["item"].(string)
		delete(watcher.inProgress, path)
	}
}

func (watcher *FsWatcher) shouldIgnore(path string) bool {
	watcher.ignoresLock.RLock()
	defer watcher.ignoresLock.RUnlock()
	return scanner.IsIgnoredPath(path, watcher.ignores) ||
		Tempnamer.IsTemporary(path)
}

func (watcher *FsWatcher) pathInProgress(path string) bool {
	_, exists := watcher.inProgress[path]
	return exists
}

func (watcher *FsWatcher) UpdateIgnores(ignores *ignore.Matcher) {
	watcher.ignoresLock.Lock()
	watcher.ignores = ignores
	watcher.ignoresLock.Unlock()
}

func (watcher *FsWatcher) debugf(text string, vals ...interface{}) {
	l.Debugf(watcher.folderID + ": " + text, vals...)
}

func (batch FsEventsBatch) GetPaths() []string {
	var paths []string
	for _, event := range batch {
		paths = append(paths, event.path)
	}
	return paths
}

func WatchesLimitTooLowError(folder string) error {
	return errors.New("Failed to install inotify handler for " +
		folder +
		". Please increase inotify limits," +
		" see http://bit.ly/1PxkdUC for more information.")
}
