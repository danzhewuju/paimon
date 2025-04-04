/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link StartingScanner} for the {@link CoreOptions.StartupMode#LATEST_FULL} startup mode. */
public class FullStartingScanner extends ReadPlanStartingScanner {

    private static final Logger LOG = LoggerFactory.getLogger(FullStartingScanner.class);

    private Snapshot startingSnapshot;

    public FullStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
        this.startingSnapshot = snapshotManager.latestSnapshot();
        if (this.startingSnapshot != null) {
            this.startingSnapshotId = startingSnapshot.id();
        }
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        if (startingSnapshot == null) {
            // try to get first snapshot again
            startingSnapshot = snapshotManager.latestSnapshot();
        }
        if (startingSnapshot == null) {
            LOG.debug("There is currently no snapshot. Waiting for snapshot generation.");
            return null;
        }
        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(startingSnapshot);
    }
}
