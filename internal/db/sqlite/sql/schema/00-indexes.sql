-- Copyright (C) 2025 The Syncthing Authors.
--
-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this file,
-- You can obtain one at https://mozilla.org/MPL/2.0/.

-- folders map folder IDs as used by Syncthing to database folder indexes
CREATE TABLE IF NOT EXISTS folders (
    idx INTEGER NOT NULL PRIMARY KEY,
    folder_id TEXT NOT NULL UNIQUE COLLATE BINARY
) STRICT
;

-- devices map device IDs as used by Syncthing to database device indexes
CREATE TABLE IF NOT EXISTS devices (
    idx INTEGER NOT NULL PRIMARY KEY,
    device_id TEXT NOT NULL UNIQUE COLLATE BINARY
) STRICT
;

-- indexids holds the index ID for a given device and folder
CREATE TABLE IF NOT EXISTS indexids (
    device_idx INTEGER NOT NULL,
    folder_idx INTEGER NOT NULL,
    index_id TEXT NOT NULL COLLATE BINARY,
    PRIMARY KEY(device_idx, folder_idx),
    FOREIGN KEY(folder_idx) REFERENCES folders(idx) ON DELETE CASCADE,
    FOREIGN KEY(device_idx) REFERENCES devices(idx) ON DELETE CASCADE
) STRICT, WITHOUT ROWID
;
