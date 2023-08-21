// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.RemoteIndexSchemaProcDir;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.QueryableReentrantReadWriteLock;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/*
 * This class is responsible for fetch variant schema from all bes.
 */
public class RemoteTableSchemaMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(RemoteTableSchemaMgr.class);

    private List<Table> tablesWithVariantColumn = Lists.newArrayList();

    // key: table id value: fetched schema from be
    private Map<Long, List<Column>> schemaCache = Maps.newHashMap();

    // protect schemaCache
    private QueryableReentrantReadWriteLock rwLock;

    public RemoteTableSchemaMgr(long intervalMs) {
        super("fetch remote table schema thread", intervalMs);
        this.rwLock = new QueryableReentrantReadWriteLock(true);
    }

    private void updateAllTableSchmea(Map<Long, List<Column>> schema) {
        this.rwLock.writeLock().lock();
        try {
            for (Map.Entry<Long, List<Column>> entry : schema.entrySet()) {
                Long tableId = entry.getKey();
                List<Column> columns = entry.getValue();
                schemaCache.put(tableId, columns);
            }
        } finally {
            this.rwLock.writeLock().unlock();
        }
        LOG.debug("update variant schema cache: {}", schemaCache);
    }

    public List<Column> getTableSchema(Long tableId) {
        this.rwLock.readLock().lock();
        try {
            return schemaCache.get(tableId);
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    public void updateOneTableSchmea(Long tableId, List<Column> columns) {
        this.rwLock.writeLock().lock();
        try {
            schemaCache.put(tableId, columns);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /*
     * For each cycle, TabletChecker will check all OlapTable's tablet.
     * If a tablet is not healthy, a TabletInfo will be created and sent to TabletScheduler for repairing.
     */
    @Override
    protected void runAfterCatalogReady() {
        clearTables();
        initTables();
        fetchAndUpdateAllTableSchmea();
    }

    private void clearTables() {
        tablesWithVariantColumn.clear();
    }

    private void fetchAndUpdateAllTableSchmea() {
        if (tablesWithVariantColumn.isEmpty()) {
            return;
        }
        // id: table id
        Map<Long, List<Column>> idToSchmea = Maps.newHashMap();
        for (Table table : tablesWithVariantColumn) {
            RemoteIndexSchemaProcDir ipd = new RemoteIndexSchemaProcDir(table, null, null);
            try {
                ipd.fetchResult();
                if (ipd.getSchema() != null) {
                    idToSchmea.put(table.getId(), ipd.getSchema());
                }
            } catch (AnalysisException e) {
                LOG.debug("fetch remote table schema failed");
            }
        }
        updateAllTableSchmea(idToSchmea);
    }

    private void initTables() {
        Env env = Env.getCurrentEnv();

        List<Long> dbIds = env.getInternalCatalog().getDbIds();
        if (dbIds.isEmpty()) {
            return;
        }
        List<Database> dbList = Lists.newArrayList();
        for (Long dbId : dbIds) {
            if (dbId == 0L) {
                // skip 'information_schema' database
                continue;
            }
            Database db = env.getInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            dbList.add(db);
        }
        List<List<Table>> tables = Lists.newArrayList();
        for (Database db : dbList) {
            db.readLock();
            try {
                tables.add(db.getTables());
            } finally {
                db.readUnlock();
            }
        }
        for (List<Table> tbs : tables) {
            for (Table table : tbs) {
                if (table.getType() != TableType.OLAP) {
                    continue;
                }
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    if (!olapTable.hasVariantColumns()) {
                        continue;
                    }
                } finally {
                    olapTable.readUnlock();
                }
                tablesWithVariantColumn.add(table);
            }
        }
    }
}
