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
	"github.com/syncthing/syncthing/lib/ignore"
)

type FsEvent struct {
	path string
	time time.Time
}

var Tempnamer scanner.TempNamer

type FsEventsBatch map[string]*FsEvent

type eventDir struct {
	events FsEventsBatch
	dirs   map[string]*eventDir
}

func newEventDir() *eventDir {
	return &eventDir{make(FsEventsBatch), make(map[string]*eventDir)}
}

type FsWatcher struct {
	folderPath            string
	notifyModelChan       chan<- FsEventsBatch
	// All detected and to be scanned events, which are grouped by their
	// parent directory to keep count of events per directory.
	fsEventDirs           map[string]*eventDir
	fsEventChan           <-chan notify.EventInfo
	WatchingFs            bool
	notifyDelay           time.Duration
	slowNotifyDelay       time.Duration
	notifyTimer           *time.Timer
	notifyTimerNeedsReset bool
	inProgress            map[string]struct{}
	folderID              string
	ignores               *ignore.Matcher
}

const (
	fastNotifyDelay = time.Duration(500) * time.Millisecond
	maxFiles        = 512
	maxFilesPerDir  = 128
)

func NewFsWatcher(folderPath string, folderID string, ignores *ignore.Matcher,
	slowNotifyDelayS int) *FsWatcher {
	if slowNotifyDelayS == 0 {
		slowNotifyDelayS = 60 * 60 * 24
	}
	return &FsWatcher{
		folderPath:            folderPath,
		notifyModelChan:       nil,
		fsEventDirs:           make(map[string]*eventDir),
		fsEventChan:           nil,
		WatchingFs:            false,
		notifyDelay:           fastNotifyDelay,
		slowNotifyDelay:       time.Duration(slowNotifyDelayS) * time.Second,
		notifyTimerNeedsReset: false,
		inProgress:            make(map[string]struct{}),
		folderID:              folderID,
		ignores:               ignores,
	}
}

func (watcher *FsWatcher) StartWatchingFilesystem() (<-chan FsEventsBatch, error) {
	fsEventChan, err := watcher.setupNotifications()
	notifyModelChan := make(chan FsEventsBatch)
	watcher.notifyModelChan = notifyModelChan
	if err == nil {
		watcher.WatchingFs = true
		watcher.fsEventChan = fsEventChan
		go watcher.watchFilesystem()
	}
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
			watcher.newFsEvent(event.Path())
		case <-watcher.notifyTimer.C:
			watcher.actOnTimer()
		case event := <-inProgressItemSubscription.C():
			watcher.updateInProgressSet(event)
		}
	}
}

func (watcher *FsWatcher) newFsEvent(eventPath string) {
	if watcher.eventCount() == maxFiles {
		watcher.debugf("Tracking too many events; dropping: %s\n", eventPath)
		return
	}
	if rootDir, ok := watcher.fsEventDirs["."]; ok {
		if _, ok := rootDir.events["."]; ok {
		        watcher.debugf("Will scan entire folder anyway; dropping: %s\n", eventPath)
			return
		}
	}
	if isSubpath(eventPath, watcher.folderPath) {
		path, _ := filepath.Rel(watcher.folderPath, eventPath)
		if watcher.pathInProgress(path) {
			watcher.debugf("Skipping notification for path we modified: %s\n",	path)
			return
		}
		if watcher.shouldIgnore(path) {
			watcher.debugf("Ignoring: %s\n", path)
			return
		}
		watcher.aggregateEvent(path, time.Now())
	} else {
		watcher.debugf("Bug: Detected change outside of folder, droping: %s\n", eventPath)
	}
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
		watcher.debugf("Resetting notifyTimer to %s\n",
			watcher.notifyDelay.String())
		watcher.notifyTimer.Reset(watcher.notifyDelay)
		watcher.notifyTimerNeedsReset = false
	}
}

func (watcher *FsWatcher) speedUpNotifyTimer() {
	if watcher.notifyDelay != fastNotifyDelay {
		watcher.notifyDelay = fastNotifyDelay
		watcher.debugf("Speeding up notifyTimer to %s\n",
			fastNotifyDelay.String())
		watcher.notifyTimerNeedsReset = true
	}
}

func (watcher *FsWatcher) slowDownNotifyTimer() {
	if watcher.notifyDelay != watcher.slowNotifyDelay {
		watcher.notifyDelay = watcher.slowNotifyDelay
		watcher.debugf("Slowing down notifyTimer to %s\n",
			watcher.notifyDelay.String())
		watcher.notifyTimerNeedsReset = true
	}
}

func (watcher *FsWatcher) aggregateEvent(path string, eventTime time.Time) {
	if path == "." {
		watcher.debugf("Aggregating: Scan entire folder")
		watcher.fsEventDirs = make(map[string]*eventDir)
		watcher.fsEventDirs["."] = newEventDir()
		watcher.fsEventDirs["."].events["."] = &FsEvent{".", eventTime}
		watcher.speedUpNotifyTimer()
		return
	}

	pathSegments := strings.Split(path, "/")

	// Events in the basepath cannot be aggregated -> allow up to maxFiles events
	localMaxFilesPerDir := maxFilesPerDir
	if len(pathSegments) == 1 {
		localMaxFilesPerDir = maxFiles
	}

	// Check if any parent directory is already tracked or will exceed
	// events per directory limit
	if _, ok := watcher.fsEventDirs["."]; !ok {
		watcher.fsEventDirs["."] = newEventDir()
	}
	parentDir := watcher.fsEventDirs["."]
	var currPath string
	for _, pathSegment := range pathSegments[:len(pathSegments) - 1] {
		currPath = filepath.Join(currPath, pathSegment)
		if _, ok := parentDir.events[currPath]; ok {
			watcher.debugf("Aggregating: Parent path already tracked: %s", path)
			return
		}
		// Is there no event already tracked below currPath?
		if _, ok := watcher.fsEventDirs[currPath]; !ok {
			watcher.fsEventDirs[currPath] = newEventDir()
			parentDir.dirs[currPath] = watcher.fsEventDirs[currPath]
			if len(parentDir.events) + len(parentDir.dirs) == localMaxFilesPerDir {
				watcher.debugf("Aggregating: Parent dir already contains %d " +
					"events, track it instead: %s",	localMaxFilesPerDir, path)
				watcher.aggregateEvent(filepath.Dir(currPath), eventTime)
				return
			}
		}
		parentDir = watcher.fsEventDirs[currPath]
	}

	if _, ok := parentDir.events[path]; ok {
		watcher.debugf("Aggregating: Parent path already tracked: %s", path)
		return
	}
	if childDir, ok := parentDir.dirs[path]; ok {
		watcher.debugf("Aggregating: Tracking and removing child events of %s", path)
		parentDir.events[path] = &FsEvent{path, childDir.getFirstModTime()}
		delete(parentDir.dirs, path)
		delete(watcher.fsEventDirs, path)
	} else {
		watcher.debugf("Aggregating: Tracking %s", path)
		parentDir.events[path] = &FsEvent{path, eventTime}
	}
	watcher.speedUpNotifyTimer()
}

func (watcher *FsWatcher) actOnTimer() {
	watcher.notifyTimerNeedsReset = true
	eventCount := watcher.eventCount()
	if eventCount == 0 {
		watcher.slowDownNotifyTimer()
		return
	}
	oldFsEvents := make(FsEventsBatch)
	if eventCount == maxFiles {
		watcher.debugf("Too many changes, issuing full rescan.")
		oldFsEvents["."] = &FsEvent{".", time.Now()}
		watcher.fsEventDirs = make(map[string]*eventDir)
	} else {
		oldFsEvents = watcher.getOldEvents(time.Now())
		watcher.removeEvents(oldFsEvents)
	}
	if len(oldFsEvents) != 0 {
		watcher.debugf("Notifying about %d fs events\n",
			len(oldFsEvents))
		watcher.notifyModelChan <- oldFsEvents
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
	return scanner.IsIgnoredPath(path, watcher.ignores) ||
		Tempnamer.IsTemporary(path)
}

func (watcher *FsWatcher) pathInProgress(path string) bool {
	_, exists := watcher.inProgress[path]
	return exists
}

func (watcher *FsWatcher) debugf(text string, vals ...interface{}) {
	l.Debugf(watcher.folderID + ": " + text, vals...)
}

func (watcher *FsWatcher) UpdateIgnores(ignores *ignore.Matcher) {
	watcher.ignores = ignores
}

func (watcher *FsWatcher) eventCount() (eventCount int) {
	for _, dir := range watcher.fsEventDirs {
		eventCount += len(dir.events)
	}
	return
}

func (watcher *FsWatcher) getOldEvents(currTime time.Time) FsEventsBatch {
	oldEvents := make(FsEventsBatch)
	for _, dir := range watcher.fsEventDirs {
		for path, event := range dir.events {
			if currTime.Sub(event.time) > fastNotifyDelay {
				watcher.debugf("Found old event %s", path)
				oldEvents[path] = event
			}
		}
	}
	return oldEvents
}

func (watcher *FsWatcher) removeEvents(events FsEventsBatch) {
	for path := range events {
		parentPath := filepath.Dir(path)
		dir := watcher.fsEventDirs[parentPath]
		watcher.debugf("Removing from fsEvents: %s", path)
		delete(dir.events, path)
		if len(dir.events) + len(dir.dirs) == 0 {
			watcher.removeDir(parentPath)
		}
	}
}

func (watcher *FsWatcher) removeDir(path string) {
	watcher.debugf("removeDir: Removing dir %s", path)
	if path != "." {
		parentPath := filepath.Dir(path)
		dir := watcher.fsEventDirs[parentPath]
		delete(dir.dirs, path)
		if len(dir.events) + len(dir.dirs) == 0 {
			watcher.removeDir(parentPath)
		}
	}
	delete(watcher.fsEventDirs, path)
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

func (dir eventDir) getFirstModTime() (firstModTime time.Time) {
	for _, childDir := range dir.dirs {
		dirTime := childDir.getFirstModTime()
		if dirTime.Before(firstModTime) {
			firstModTime = dirTime
		}
	}
	for _, event := range dir.events {
		if event.time.Before(firstModTime) {
			firstModTime = event.time
		}
	}
	return
}
