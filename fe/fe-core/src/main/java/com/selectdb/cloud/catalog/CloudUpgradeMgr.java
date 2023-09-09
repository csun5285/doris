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

package com.selectdb.cloud.catalog;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.transaction.CloudGlobalTransactionMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class CloudUpgradeMgr extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(CloudUpgradeMgr.class);
    /*   (<(<dbid1, txn1>, <dbid2, txn2>, ... <dbid3, txn3>), be>, <...>, ...) */
    private LinkedBlockingQueue<Pair<LinkedBlockingQueue<Pair<Long, Long>>, Long>> txnBePairList =
            new LinkedBlockingQueue<>();

    public CloudUpgradeMgr() {
        super("cloud upgrade manager", Config.cloud_upgrade_mgr_interval_second * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        LOG.info("upgrade mgr"); // TODO: delete
        Iterator<Pair<LinkedBlockingQueue<Pair<Long, Long>>, Long>> pairIter = txnBePairList.iterator();
        while (pairIter.hasNext()) {
            Pair<LinkedBlockingQueue<Pair<Long, Long>>, Long> pair = pairIter.next();
            LinkedBlockingQueue<Pair<Long, Long>> txnList = pair.first;
            long be = pair.second;

            boolean isFinished = false;
            boolean isBeInactive = true;
            Iterator<Pair<Long, Long>> txnpairIter = txnList.iterator();
            while (txnpairIter.hasNext()) {
                Pair<Long, Long> txnpair = txnpairIter.next();
                List<Long> tableIdList = getAllTables(); // TODO: do it once?
                if (tableIdList.isEmpty()) {
                    /* no table in this cluster */
                    break;
                }
                try {
                    if (Config.enable_running_txn_check) {
                        isFinished = ((CloudGlobalTransactionMgr) Env.getCurrentGlobalTransactionMgr())
                                .isPreviousNonTimeoutTxnFinished(txnpair.second, txnpair.first, tableIdList);
                    } else {
                        isFinished = true;
                    }
                } catch (AnalysisException e) {
                    throw new RuntimeException(e);
                }
                if (!isFinished) {
                    isBeInactive = false;
                    LOG.info("BE {} is still active, waiting txn {}", be, txnpair);
                    break;
                }
            }
            if (isBeInactive) {
                setBeStateInactive(be);
                LOG.info("BE {} is inactive", be);
                txnBePairList.remove(pair);
            }
        }
    }

    /* called after tablets migrating to new BE process complete */
    public void registerWaterShedTxnId(long be) throws AnalysisException {
        LinkedBlockingQueue<Pair<Long, Long>> txnids = new LinkedBlockingQueue<>();
        List<Long> dbids = Env.getCurrentInternalCatalog().getDbIds();
        for (long dbid : dbids) {
            txnids.offer(Pair.of(dbid, Env.getCurrentGlobalTransactionMgr().getNextTransactionId(dbid)));
        }
        txnBePairList.offer(Pair.of(txnids, be));
        LOG.info("register watershedtxnid {} for BE {}", txnids, be);
    }

    private List<Long> getAllTables() {
        List<Long> mergedList = new ArrayList<>();

        List<Long> dbIds = Env.getCurrentInternalCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            db.readLock();
            List<Long> tableList = db.getTableIds();
            db.readUnlock();
            mergedList.addAll(tableList);
        }
        return mergedList;
    }

    public void setBeStateInactive(long beId) {
        Backend be = Env.getCurrentSystemInfo().getBackend(beId);
        if (be == null) {
            LOG.warn("cannot get be {} to set inactive state", beId);
            return;
        }
        be.setActive(false); /* now user can get BE inactive status from `show backends;` */
        Env.getCurrentEnv().getEditLog().logModifyBackend(be);
        LOG.info("finished to modify backend {} ", be);
    }
}
