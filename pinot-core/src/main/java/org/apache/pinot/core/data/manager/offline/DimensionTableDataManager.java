/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.offline;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.manager.config.TableDataManagerConfig;
import org.apache.pinot.core.data.readers.MultiplePinotSegmentRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


/**
 * Dimension Table is a special type of OFFLINE table which is assigned to all servers
 * and is used to execute a LOOKUP Transform Function. They should be small enough to fit in memory(<100MB).
 *
 * DimensionTableDataManager uses Registry of Singletons pattern to store one instance per table
 * which can be accessed via 'getInstanceByTableName' static method.
 */
public class DimensionTableDataManager extends OfflineTableDataManager {
  // Store singletons per table in this map
  private static final Map<String, DimensionTableDataManager> _instances = new ConcurrentHashMap<>();

  public static DimensionTableDataManager createInstanceByTableName(String tableName) {
    _instances.putIfAbsent(tableName, new DimensionTableDataManager());
    return _instances.get(tableName);
  }

  public static DimensionTableDataManager getInstanceByTableName(String tableName) {
    return _instances.get(tableName);
  }

  // DimensionTableDataManager Instance properties
  //

  // TODO this map needs to be periodically recycled
  // _lookupTable is a HashMap used for fetching records from a table given the primary key
  private final Map<PrimaryKey, GenericRow> _lookupTable = new HashMap<>();
  private final ReadWriteLock _rwl = new ReentrantReadWriteLock();
  private final Lock _lookupTableReadLock = _rwl.readLock();
  private final Lock _lookupTableWriteLock = _rwl.writeLock();

  @Override
  public void init(TableDataManagerConfig tableDataManagerConfig, String instanceId,
      ZkHelixPropertyStore<ZNRecord> propertyStore, ServerMetrics serverMetrics, HelixManager helixManager) {
    super.init(tableDataManagerConfig, instanceId, propertyStore, serverMetrics, helixManager);
    try {
      prepareLookupTable();
      _logger.info("Successfully loaded lookup table for {}", getTableName());
    } catch (Exception e) {
      _logger.error("Unable to load lookup table {}\nError: {}\n",
          getTableName(), e.getCause(), e);
    }
  }

  private void prepareLookupTable() throws Exception {
    List<SegmentDataManager> segmentManagers = acquireAllSegments();
    List<File> indexDirs = new ArrayList<>();
    List<String> primaryKeyColumns = new ArrayList<>();

    for (SegmentDataManager segmentManager: segmentManagers) {
      IndexSegment indexSegment = segmentManager.getSegment();
      indexDirs.add(indexSegment.getSegmentMetadata().getIndexDir());
      primaryKeyColumns = indexSegment.getSegmentMetadata().getSchema().getPrimaryKeyColumns();
    }
    MultiplePinotSegmentRecordReader reader = new MultiplePinotSegmentRecordReader(indexDirs);

    // TODO validate primary key columns exist
    if (primaryKeyColumns == null) {
      primaryKeyColumns = new ArrayList<>(Arrays.asList("teamID"));
    }

    _lookupTableWriteLock.lock();
    try {
      _lookupTable.clear();
      while (reader.hasNext()) {
        GenericRow row = reader.next();
        _lookupTable.put(row.getPrimaryKey(primaryKeyColumns), row);
      }
    } finally {
      _lookupTableWriteLock.unlock();
    }
  }

  public GenericRow lookupRowByPrimaryKey(PrimaryKey pk) {
    _lookupTableReadLock.lock();
    try {
      return _lookupTable.get(pk);
    } finally {
      _lookupTableReadLock.unlock();
    }
  }
}
