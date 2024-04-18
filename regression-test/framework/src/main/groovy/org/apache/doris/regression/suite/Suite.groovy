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

package org.apache.doris.regression.suite

import groovy.json.JsonOutput
import com.google.common.collect.Maps
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors
import com.google.gson.Gson
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import com.google.common.collect.ImmutableList
import org.apache.doris.regression.Config
import org.apache.doris.regression.action.BenchmarkAction
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.OutputUtils
import org.apache.doris.regression.action.CreateMVAction
import org.apache.doris.regression.action.ExplainAction
import org.apache.doris.regression.action.HttpCliAction
import org.apache.doris.regression.action.RestoreAction
import org.apache.doris.regression.action.StreamLoadAction
import org.apache.doris.regression.action.SuiteAction
import org.apache.doris.regression.action.TestAction
import org.apache.doris.regression.util.DataUtils
import org.apache.doris.regression.util.Hdfs
import org.apache.doris.regression.util.JdbcUtils
import org.junit.Assert
import org.apache.doris.regression.util.SuiteUtils
import org.apache.doris.regression.util.DebugPoint
import org.junit.jupiter.api.Assertions
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.File
import java.math.BigDecimal;
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import java.util.Map;
import java.util.concurrent.Callable
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors
import java.util.stream.LongStream
import static org.apache.doris.regression.util.DataUtils.sortByToString

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSetMetaData
import org.junit.Assert


@Slf4j
class Suite implements GroovyInterceptable {
    final SuiteContext context
    final String name
    final String group
    final Logger logger = LoggerFactory.getLogger(this.class)

    final List<Closure> successCallbacks = new Vector<>()
    final List<Closure> failCallbacks = new Vector<>()
    final List<Closure> finishCallbacks = new Vector<>()
    final List<Throwable> lazyCheckExceptions = new Vector<>()
    final List<Future> lazyCheckFutures = new Vector<>()

    static final List<Tuple2<String, String>> dorisSelectdbDiffCfgNames = [
        // each tuple: [doris config,  selectdb config]
        ['cloud_balance_tablet_percent_per_run', 'balance_tablet_percent_per_run'],
        ['enable_cloud_global_balance', 'enable_global_balance'],
        ['cloud_pre_heating_time_limit_sec', 'pre_heating_time_limit_sec'],

        // TODO: after doris support preheating, doris config name use `cloud_preheating_enabled`
        ['', 'preheating_enabled'],
    ]

    SuiteCluster cluster
    DebugPoint debugPoint

    Suite(String name, String group, SuiteContext context) {
        this.name = name
        this.group = group
        this.context = context
        this.cluster = null
        this.debugPoint = new DebugPoint(this)
    }

    String getConf(String key, String defaultValue = null) {
        String value = context.config.otherConfigs.get(key)
        return value == null ? defaultValue : value
    }

    Properties getConfs(String prefix) {
        Properties p = new Properties()
        for (String name : context.config.otherConfigs.stringPropertyNames()) {
            if (name.startsWith(prefix + ".")) {
                p.put(name.substring(prefix.length() + 1), context.config.getProperty(name))
            }
        }
        return p
    }

    void onSuccess(Closure callback) {
        successCallbacks.add(callback)
    }

    void onFail(Closure callback) {
        failCallbacks.add(callback)
    }

    void onFinish(Closure callback) {
        finishCallbacks.add(callback)
    }

    LongStream range(long startInclusive, long endExclusive) {
        return LongStream.range(startInclusive, endExclusive)
    }

    LongStream rangeClosed(long startInclusive, long endInclusive) {
        return LongStream.rangeClosed(startInclusive, endInclusive)
    }

    String toCsv(List<Object> rows) {
        StringBuilder sb = new StringBuilder()
        for (int i = 0; i < rows.size(); ++i) {
            Object row = rows.get(i)
            if (!(row instanceof List)) {
                row = ImmutableList.of(row)
            }
            sb.append(OutputUtils.toCsvString(row as List)).append("\n")
        }
        sb.toString()
    }

    Object parseJson(String str) {
        def jsonSlurper = new JsonSlurper()
        return jsonSlurper.parseText(str)
    }

    public <T> T lazyCheck(Closure<T> closure) {
        try {
            T result = closure.call()
            if (result instanceof Future) {
                lazyCheckFutures.add(result)
            }
            return result
        } catch (Throwable t) {
            lazyCheckExceptions.add(t)
            return null
        }
    }

    void doLazyCheck() {
        if (!lazyCheckExceptions.isEmpty()) {
            throw lazyCheckExceptions.get(0)
        }
        lazyCheckFutures.forEach { it.get() }
    }

    public <T> Tuple2<T, Long> timer(Closure<T> actionSupplier) {
        return SuiteUtils.timer(actionSupplier)
    }

    public <T> ListenableFuture<T> thread(String threadName = null, Closure<T> actionSupplier) {
        return MoreExecutors.listeningDecorator(context.actionExecutors).submit((Callable<T>) {
            long startTime = System.currentTimeMillis()
            def originThreadName = Thread.currentThread().name
            try {
                Thread.currentThread().setName(threadName == null ? originThreadName : threadName)
                context.scriptContext.eventListeners.each { it.onThreadStarted(context) }

                return actionSupplier.call()
            } catch (Throwable t) {
                context.scriptContext.eventListeners.each { it.onThreadFailed(context, t) }
                throw t
            } finally {
                try {
                    context.closeThreadLocal()
                } catch (Throwable t) {
                    logger.warn("Close thread local context failed", t)
                }
                long finishTime = System.currentTimeMillis()
                context.scriptContext.eventListeners.each { it.onThreadFinished(context, finishTime - startTime) }
                Thread.currentThread().setName(originThreadName)
            }
        })
    }

    public <T> ListenableFuture<T> lazyCheckThread(String threadName = null, Closure<T> actionSupplier) {
        return lazyCheck {
            thread(threadName, actionSupplier)
        }
    }

    public <T> ListenableFuture<T> combineFutures(ListenableFuture<T> ... futures) {
        return Futures.allAsList(futures)
    }

    public <T> ListenableFuture<List<T>> combineFutures(Iterable<? extends ListenableFuture<? extends T>> futures) {
        return Futures.allAsList(futures)
    }

    public <T> T connect(String user = context.config.jdbcUser, String password = context.config.jdbcPassword,
                         String url = context.config.jdbcUrl, Closure<T> actionSupplier) {
        return context.connect(user, password, url, actionSupplier)
    }

    public void docker(ClusterOptions options = new ClusterOptions(), Closure actionSupplier) throws Exception {
        if (context.config.excludeDockerTest) {
            return
        }

        cluster = new SuiteCluster(name, context.config)
        try {
            cluster.destroy(true)
            cluster.init(options)

            def user = "root"
            def password = ""
            def masterFe = cluster.getMasterFe()
            def url = String.format(
                    "jdbc:mysql://%s:%s/?useLocalSessionState=false&allowLoadLocalInfile=false",
                    masterFe.host, masterFe.queryPort)
            def conn = DriverManager.getConnection(url, user, password)
            def sql = "CREATE DATABASE IF NOT EXISTS " + context.dbName
            logger.info("try create database if not exists {}", context.dbName)
            JdbcUtils.executeToList(conn, sql)
            url = Config.buildUrlWithDb(url, context.dbName)

            logger.info("connect to docker cluster: suite={}, url={}", name, url)
            connect(user, password, url, actionSupplier)
        } finally {
            cluster.destroy(context.config.dockerEndDeleteFiles)
        }
    }

    String get_ccr_body(String table) {
        Gson gson = new Gson()

        Map<String, String> srcSpec = context.getSrcSpec()
        srcSpec.put("table", table)

        Map<String, String> destSpec = context.getDestSpec()
        destSpec.put("table", table)

        Map<String, Object> body = Maps.newHashMap()
        body.put("name", context.suiteName + "_" + context.dbName + "_" + table)
        body.put("src", srcSpec)
        body.put("dest", destSpec)

        return gson.toJson(body)
    }

    Syncer getSyncer() {
        return context.getSyncer(this)
    }

    List<List<Object>> sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

<<<<<<< HEAD
    List<List<Object>> insert_into_sql(String sqlStr, int num) {
        logger.info("insert into " + num + " records")
        def (result, meta) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        return result
    }

    List<List<Object>> exec(Object stmt) {
        logger.info("Execute sql: ${stmt}".toString())
        def (result, meta )= JdbcUtils.executeToList(context.getConnection(),  (PreparedStatement) stmt)
        return result
    }

    def sql_return_maparray(String sqlStr) {
        logger.info("Execute sql: ${sqlStr}".toString())
        return JdbcUtils.executeToMapArray(context.getConnection(), sqlStr)
=======
    List<List<Object>> insert_into_sql_impl(Connection conn, String sqlStr, int num) {
        logger.info("insert into " + num + " records")
        def (result, meta) = JdbcUtils.executeToList(conn, sqlStr)
        return result
    }

    List<List<Object>> jdbc_insert_into_sql(String sqlStr, int num) {
        return insert_into_sql_impl(context.getConnection(), sqlStr, num)
    }

    List<List<Object>> arrow_flight_insert_into_sql(String sqlStr, int num) {
        return insert_into_sql_impl(context.getArrowFlightSqlConnection(), (String) ("USE ${context.dbName};" + sqlStr), num)
    }

    List<List<Object>> insert_into_sql(String sqlStr, int num) {
        if (context.useArrowFlightSql()) {
            return arrow_flight_insert_into_sql(sqlStr, num)
        } else {
            return jdbc_insert_into_sql(sqlStr, num)
        }
    }

    def sql_return_maparray(String sqlStr, Connection conn = null) {        
        logger.info("Execute sql: ${sqlStr}".toString())
        if (conn == null) {
            conn = context.getConnection()
        }
        def (result, meta) = JdbcUtils.executeToList(conn, sqlStr)

        // get all column names as list
        List<String> columnNames = new ArrayList<>()
        for (int i = 0; i < meta.getColumnCount(); i++) {
            columnNames.add(meta.getColumnName(i + 1))
        }

        // add result to res map list, each row is a map with key is column name
        List<Map<String, Object>> res = new ArrayList<>()
        for (int i = 0; i < result.size(); i++) {
            Map<String, Object> row = new HashMap<>()
            for (int j = 0; j < columnNames.size(); j++) {
                row.put(columnNames.get(j), result.get(i).get(j))
            }
            res.add(row)
        }
        return res;
>>>>>>> b15854a19f
    }

    List<List<Object>> target_sql(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}target_sql: ${sqlStr}".toString())
        def (result, meta) = JdbcUtils.executeToList(context.getTargetConnection(this), sqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<String>> sql_meta(String sqlStr, boolean isOrder = false) {
        logger.info("Execute ${isOrder ? "order_" : ""}sql: ${sqlStr}".toString())
        def (tmp, rsmd) = JdbcUtils.executeToList(context.getConnection(), sqlStr)
        int count = rsmd.getColumnCount();
        List<List<String>> result = new ArrayList<>()
        for (int i = 0; i < count; i++) {
            List<String> item = new ArrayList<>()
            String columnName = rsmd.getColumnName(i + 1);
            int columnType = rsmd.getColumnType(i+1);
            String columnTypeName = rsmd.getColumnTypeName(i+1);
            item.add(columnName);
            item.add(columnTypeName);
            result.add(item);
        }
        return result;
    }

    List<List<Object>> order_sql(String sqlStr) {
        return sql(sqlStr,  true)
    }

    List<List<Object>> sortRows(List<List<Object>> result) {
        if (result == null) {
            return null
        }
        return DataUtils.sortByToString(result)
    }

    String selectUnionAll(List list) {
        def toSelectString = { Object value ->
            if (value == null) {
                return "null"
            } else if (value instanceof Number) {
                return value.toString()
            } else {
                return "'${value.toString()}'".toString()
            }
        }
        AtomicBoolean isFirst = new AtomicBoolean(true)
        String sql = list.stream()
            .map({ row ->
                StringBuilder sb = new StringBuilder("SELECT ")
                if (row instanceof List) {
                    if (isFirst.get()) {
                        String columns = row.withIndex().collect({ column, index ->
                            "${toSelectString(column)} AS c${index + 1}"
                        }).join(", ")
                        sb.append(columns)
                        isFirst.set(false)
                    } else {
                        String columns = row.collect({ column ->
                            "${toSelectString(column)}"
                        }).join(", ")
                        sb.append(columns)
                    }
                } else {
                    if (isFirst.get()) {
                        sb.append(toSelectString(row)).append(" AS c1")
                        isFirst.set(false)
                    } else {
                        sb.append(toSelectString(row))
                    }
                }
                return sb.toString()
            }).collect(Collectors.joining("\nUNION ALL\n"))
        return sql
    }

    void explain(Closure actionSupplier) {
        runAction(new ExplainAction(context), actionSupplier)
    }

    void createMV(String sql) {
        (new CreateMVAction(context, sql)).run()
    }

    void createMV(String sql, String expection) {
        (new CreateMVAction(context, sql, expection)).run()
    }

    void test(Closure actionSupplier) {
        runAction(new TestAction(context), actionSupplier)
    }

    void benchmark(Closure actionSupplier) {
        runAction(new BenchmarkAction(context), actionSupplier)
    }

    String getBrokerName() {
        String brokerName = context.config.otherConfigs.get("brokerName")
        return brokerName
    }

    String getHdfsFs() {
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        return hdfsFs
    }

    String getHdfsUser() {
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        return hdfsUser
    }

    String getHdfsPasswd() {
        String hdfsPasswd = context.config.otherConfigs.get("hdfsPasswd")
        return hdfsPasswd
    }

    String getHdfsDataDir() {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.genRemoteDataDir()
    }

    boolean enableHdfs() {
        String enableHdfs = context.config.otherConfigs.get("enableHdfs");
        return enableHdfs.equals("true");
    }

    String uploadToHdfs(String localFile) {
        // as group can be rewrite the origin data file not relate to group
        String dataDir = context.config.dataPath + "/"
        localFile = dataDir + localFile
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        String remotePath = hdfs.upload(localFile)
        return remotePath;
    }

    String getLoalFilePath(String fileName) {
        if (!new File(fileName).isAbsolute()) {
            fileName = new File(context.dataPath, fileName).getAbsolutePath()
        }
        def file = new File(fileName)
        if (!file.exists()) {
            log.warn("Stream load input file not exists: ${file}".toString())
            throw new IllegalStateException("Stream load input file not exists: ${file}");
        }
        def localFile = file.canonicalPath
        log.info("Set stream load input: ${file.canonicalPath}".toString())
        return localFile;
    }

    boolean enableBrokerLoad() {
        String enableBrokerLoad = context.config.otherConfigs.get("enableBrokerLoad");
        return (enableBrokerLoad != null && enableBrokerLoad.equals("true"));
    }

    String getSmokeEnv() {
        String env = context.config.otherConfigs.get("cloudSmokeEnv");
        return env
    }

    String getS3Region() {
        String s3Region = context.config.otherConfigs.get("s3Region");
        return s3Region
    }

    String getS3BucketName() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        return s3BucketName
    }

    String getS3Endpoint() {
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        return s3Endpoint
    }

    String getS3AK() {
        String ak = context.config.otherConfigs.get("ak");
        return ak
    }

    String getS3SK() {
        String sk = context.config.otherConfigs.get("sk");
        return sk
    }

    String getS3Prefix() {
        return context.config.otherConfigs.get("s3Prefix");
    }

    String getS3Url() {
        String s3BucketName = context.config.otherConfigs.get("s3BucketName");
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint");
        String s3Url = "http://${s3BucketName}.${s3Endpoint}"
        return s3Url
    }

    void scpFiles(String username, String host, String files, String filePath, boolean fromDst=true) {
        String cmd = "scp -r ${username}@${host}:${files} ${filePath}"
        if (!fromDst) {
            cmd = "scp -r ${files} ${username}@${host}:${filePath}"
        }
        logger.info("Execute: ${cmd}".toString())
        Process process = cmd.execute()
        def code = process.waitFor()
        Assert.assertEquals(0, code)
    }

    void sshExec(String username, String host, String cmd) {
        String command = "ssh ${username}@${host} '${cmd}'"
        def cmds = ["/bin/bash", "-c", command]
        logger.info("Execute: ${cmds}".toString())
        Process p = cmds.execute()
        def errMsg = new StringBuilder()
        def msg = new StringBuilder()
        p.waitForProcessOutput(msg, errMsg)
        assert errMsg.length() == 0: "error occurred!" + errMsg
        assert p.exitValue() == 0
    }


    void getBackendIpHttpPort(Map<String, String> backendId_to_backendIP, Map<String, String> backendId_to_backendHttpPort) {
        List<List<Object>> backends = sql("show backends");
        String backend_id;
        for (List<Object> backend : backends) {
            backendId_to_backendIP.put(String.valueOf(backend[0]), String.valueOf(backend[1]));
            backendId_to_backendHttpPort.put(String.valueOf(backend[0]), String.valueOf(backend[4]));
        }
        return;
    }

    String getProvider() {
        String s3Endpoint = context.config.otherConfigs.get("s3Endpoint")
        return getProvider(s3Endpoint)
    }

    String getProvider(String endpoint) {
        def providers = ["cos", "oss", "s3", "obs", "bos"]
        for (final def provider in providers) {
            if (endpoint.containsIgnoreCase(provider)) {
                return provider
            }
        }
        return ""
    }

    int getTotalLine(String filePath) {
        def file = new File(filePath)
        int lines = 0;
        file.eachLine {
            lines++;
        }
        return lines;
    }


    Connection getTargetConnection() {
        return context.getTargetConnection(this)
    }
    
    boolean deleteFile(String filePath) {
        def file = new File(filePath)
        file.delete()
    }

    void waitingMTMVTaskFinished(String mvName) {
        String showTasks = "SHOW MTMV TASK ON " + mvName
        List<List<String>> showTaskMetaResult = sql_meta(showTasks)
        int index = showTaskMetaResult.indexOf(['State', 'CHAR'])
        String status = "PENDING"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 30 * 60 * 1000 // 30 min
        do {
            result = sql(showTasks)
            if (!result.isEmpty()) {
                status = result.last().get(index)
            }
            println "The state of ${showTasks} is ${status}"
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING'))
        if (status != "SUCCESS") {
            println "status is not success"
            println result.toString()
        }
        Assert.assertEquals("SUCCESS", status)
    }

    List<String> downloadExportFromHdfs(String label) {
        String dataDir = context.config.dataPath + "/" + group + "/"
        String hdfsFs = context.config.otherConfigs.get("hdfsFs")
        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        Hdfs hdfs = new Hdfs(hdfsFs, hdfsUser, dataDir)
        return hdfs.downLoad(label)
    }

    void streamLoad(Closure actionSupplier) {
        runAction(new StreamLoadAction(context), actionSupplier)
    }

    void restore(Closure actionSupplier) {
        runAction(new RestoreAction(context), actionSupplier)
    }

    void httpTest(Closure actionSupplier) {
        runAction(new HttpCliAction(context), actionSupplier)
    }

    void runAction(SuiteAction action, Closure actionSupplier) {
        actionSupplier.setDelegate(action)
        actionSupplier.setResolveStrategy(Closure.DELEGATE_FIRST)
        actionSupplier.call(action)
        action.run()
    }

    PreparedStatement prepareStatement(String sql) {
        logger.info("Execute sql: ${sql}".toString())
        return JdbcUtils.prepareStatement(context.getConnection(), sql)
    }

    List<List<Object>> hive_docker(String sqlStr, boolean isOrder = false){
        String cleanedSqlStr = sqlStr.replaceAll("\\s*;\\s*\$", "")
        def (result, meta) = JdbcUtils.executeToList(context.getHiveDockerConnection(), cleanedSqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    List<List<Object>> hive_remote(String sqlStr, boolean isOrder = false){
        String cleanedSqlStr = sqlStr.replaceAll("\\s*;\\s*\$", "")
        def (result, meta) = JdbcUtils.executeToList(context.getHiveRemoteConnection(), cleanedSqlStr)
        if (isOrder) {
            result = DataUtils.sortByToString(result)
        }
        return result
    }

    void quickRunTest(String tag, Object arg, boolean isOrder = false) {
        logger.info("Execute tag: ${tag}, ${isOrder ? "order_" : ""}sql: ${arg}".toString())
        if (context.config.generateOutputFile || context.config.forceGenerateOutputFile) {
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(),  (PreparedStatement) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(),  (PreparedStatement) arg)
                }
                else{
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
                }
            } else {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(), (String) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(), (String) arg)
                }
                else{
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
                }
            }
            def (result, meta) = tupleResult
            if (isOrder) {
                result = sortByToString(result)
            }
            Iterator<List<Object>> realResults = result.iterator()
            // generate and save to .out file
            def writer = context.getOutputWriter(context.config.forceGenerateOutputFile)
            writer.write(realResults, tag)
        } else {
            if (!context.outputFile.exists()) {
                throw new IllegalStateException("Missing outputFile: ${context.outputFile.getAbsolutePath()}")
            }

            if (!context.getOutputIterator().hasNextTagBlock(tag)) {
                throw new IllegalStateException("Missing output block for tag '${tag}': ${context.outputFile.getAbsolutePath()}")
            }

            OutputUtils.TagBlockIterator expectCsvResults = context.getOutputIterator().next()
            Tuple2<List<List<Object>>, ResultSetMetaData> tupleResult = null
            if (arg instanceof PreparedStatement) {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(),  (PreparedStatement) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(),  (PreparedStatement) arg)
                }
                else{
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (PreparedStatement) arg)
                }
            } else {
                if (tag.contains("hive_docker")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveDockerConnection(), (String) arg)
                }else if (tag.contains("hive_remote")) {
                    tupleResult = JdbcUtils.executeToStringList(context.getHiveRemoteConnection(), (String) arg)
                }
                else{
                    tupleResult = JdbcUtils.executeToStringList(context.getConnection(),  (String) arg)
                }
            }
            def (realResults, meta) = tupleResult
            if (isOrder) {
                realResults = sortByToString(realResults)
            }

            Iterator<List<Object>> realResultsIter = realResults.iterator()
            def realWriter = context.getRealOutputWriter(true)
            realWriter.write(realResultsIter, tag)

            String errorMsg = null
            try {
                errorMsg = OutputUtils.checkOutput(expectCsvResults, realResults.iterator(),
                    { row -> OutputUtils.toCsvString(row as List<Object>) },
                    { row ->  OutputUtils.toCsvString(row) },
                    "Check tag '${tag}' failed", meta)
            } catch (Throwable t) {
                throw new IllegalStateException("Check tag '${tag}' failed, sql:\n${arg}", t)
            }
            if (errorMsg != null) {
                logger.warn("expect results: " + expectCsvResults + "\nrealResults: " + realResults)
                throw new IllegalStateException("Check tag '${tag}' failed:\n${errorMsg}\n\nsql:\n${arg}")
            }
        }
    }

    void quickTest(String tag, String sql, boolean isOrder = false) {
        logger.info("Execute tag: ${tag}, ${isOrder ? "order_" : ""}sql: ${sql}".toString())
        if (tag.contains("hive_docker")) {
            String cleanedSqlStr = sql.replaceAll("\\s*;\\s*\$", "")
            sql = cleanedSqlStr
        }
        if (tag.contains("hive_remote")) {
            String cleanedSqlStr = sql.replaceAll("\\s*;\\s*\$", "")
            sql = cleanedSqlStr
        }
        quickRunTest(tag, sql, isOrder)
    }

    void quickExecute(String tag, PreparedStatement stmt) {
        logger.info("Execute tag: ${tag}, sql: ${stmt}".toString())
        quickRunTest(tag, stmt)
    }

    @Override
    Object invokeMethod(String name, Object args) {
        // qt: quick test
        if (name.startsWith("qt_")) {
            return quickTest(name.substring("qt_".length()), (args as Object[])[0] as String)
        } else if (name.startsWith("order_qt_")) {
            return quickTest(name.substring("order_qt_".length()), (args as Object[])[0] as String, true)
        } else if (name.startsWith("qe_")) {
            return quickExecute(name.substring("qe_".length()), (args as Object[])[0] as PreparedStatement)
        } else if (name.startsWith("assert") && name.length() > "assert".length()) {
            // delegate to junit Assertions dynamically
            return Assertions."$name"(*args) // *args: spread-dot
        } else if (name.startsWith("try_")) {
            String realMethod = name.substring("try_".length())
            try {
                return this."$realMethod"(*args)
            } catch (Throwable t) {
                // do nothing
                return null
            }
        } else {
            // invoke origin method
            return metaClass.invokeMethod(this, name, args)
        }
    }

    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance
    def get_be_metric = { ip, port, field ->
        def metric_api = { request_body, check_func ->
            httpTest {
                endpoint ip + ":" + port
                uri "/metrics?type=json"
                body request_body
                op "get"
                check check_func
            }
        }

        def jsonOutput = new JsonOutput()
        def map = []
        def js = jsonOutput.toJson(map)
        log.info("get be metric req: ${js} ".toString())

        def ret = 0;
        metric_api.call(js) {
            respCode, body ->
                log.info("get be metric resp: ${respCode}".toString())
                def json = parseJson(body)
                for (item : json) {
                    if (item.tags.metric == field) {
                        ret = item.value
                    }
                }
        }
        ret
    }

    def add_cluster = { be_unique_id, ip, port, cluster_name, cluster_id ->
        def jsonOutput = new JsonOutput()
        def s3 = [
                     type: "COMPUTE",
                     cluster_name : cluster_name,
                     cluster_id : cluster_id,
                     nodes: [
                         [
                             cloud_unique_id: be_unique_id,
                             ip: ip,
                             heartbeat_port: port
                         ],
                     ]
                 ]
        def map = [instance_id: "${instance_id}", cluster: s3]
        def js = jsonOutput.toJson(map)
        log.info("add cluster req: ${js} ".toString())

        def add_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/add_cluster?token=${token}"
                body request_body
                check check_func
            }
        }

        add_cluster_api.call(js) {
            respCode, body ->
                log.info("add cluster resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }
    }

    def get_cluster = { be_unique_id ->
        def jsonOutput = new JsonOutput()
        def map = [instance_id: "${instance_id}", cloud_unique_id: "${be_unique_id}" ]
        def js = jsonOutput.toJson(map)
        log.info("get cluster req: ${js} ".toString())

        def add_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/get_cluster?token=${token}"
                body request_body
                check check_func
            }
        }

        def json
        add_cluster_api.call(js) {
            respCode, body ->
                log.info("get cluster resp: ${body} ${respCode}".toString())
                json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }
        json.result.cluster
    }

    def drop_cluster = { cluster_name, cluster_id ->
        def jsonOutput = new JsonOutput()
        def reqBody = [
                     type: "COMPUTE",
                     cluster_name : cluster_name,
                     cluster_id : cluster_id,
                     nodes: [
                     ]
                 ]
        def map = [instance_id: "${instance_id}", cluster: reqBody]
        def js = jsonOutput.toJson(map)
        log.info("drop cluster req: ${js} ".toString())

        def drop_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/drop_cluster?token=${token}"
                body request_body
                check check_func
            }
        }

        drop_cluster_api.call(js) {
            respCode, body ->
                log.info("dorp cluster resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }
    }

    def add_node = { be_unique_id, ip, port, cluster_name, cluster_id ->
        def jsonOutput = new JsonOutput()
        def clusterInfo = [
                     type: "COMPUTE",
                     cluster_name : cluster_name,
                     cluster_id : cluster_id,
                     nodes: [
                         [
                             cloud_unique_id: be_unique_id,
                             ip: ip,
                             heartbeat_port: port
                         ],
                     ]
                 ]
        def map = [instance_id: "${instance_id}", cluster: clusterInfo]
        def js = jsonOutput.toJson(map)
        log.info("add node req: ${js} ".toString())

        def add_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/add_node?token=${token}"
                body request_body
                check check_func
            }
        }

        add_cluster_api.call(js) {
            respCode, body ->
                log.info("add node resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }
    }

    def d_node = { be_unique_id, ip, port, cluster_name, cluster_id ->
        def jsonOutput = new JsonOutput()
        def clusterInfo = [
                     type: "COMPUTE",
                     cluster_name : cluster_name,
                     cluster_id : cluster_id,
                     nodes: [
                         [
                             cloud_unique_id: be_unique_id,
                             ip: ip,
                             heartbeat_port: port
                         ],
                     ]
                 ]
        def map = [instance_id: "${instance_id}", cluster: clusterInfo]
        def js = jsonOutput.toJson(map)
        log.info("decommission node req: ${js} ".toString())

        def d_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/decommission_node?token=${token}"
                body request_body
                check check_func
            }
        }

        d_cluster_api.call(js) {
            respCode, body ->
                log.info("decommission node resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK") || json.code.equalsIgnoreCase("ALREADY_EXISTED"))
        }
    }

    def checkProfile = { addrSet ->
        List<List<Object>> profileRes = sql " show query profile '/' "
        for (row : profileRes) {
            // println row
        }

        int i = 0
        while (true) {
            String exec_sql = "show query profile '/" + profileRes[0][0] + "/" + i.toString() + "'"
            try {
                List<List<Object>> result = sql exec_sql
                for (row : result) {
                    println row
                }

                println result[0][1]
                println addrSet
                assertTrue(addrSet.contains(result[0][1]));
            } catch (Exception ex) {
                if (ex.getMessage().contains("Failed to get instance list for fragment")) {
                    break
                }
                throw ex
            }
            i++
        }
    }

    def rename_cloud_cluster = { cluster_name, cluster_id ->
        def jsonOutput = new JsonOutput()
        def reqBody = [
                          cluster_name : cluster_name,
                          cluster_id : cluster_id
                      ]
        def map = [instance_id: "${instance_id}", cluster: reqBody]
        def js = jsonOutput.toJson(map)
        log.info("rename cluster req: ${js} ".toString())

        def rename_cluster_api = { request_body, check_func ->
            httpTest {
                endpoint context.config.metaServiceHttpAddress
                uri "/MetaService/http/rename_cluster?token=${token}"
                body request_body
                check check_func
            }
        }

        rename_cluster_api.call(js) {
            respCode, body ->
                log.info("rename cluster resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
        }
    }

    void wait_cluster_change() {
        sleep(20000)
    }

    public void resetConnection() {
        context.resetConnection()
    }
    
    Boolean checkSnapshotFinish() {
        String checkSQL = "SHOW BACKUP FROM " + context.dbName
        int size = sql(checkSQL).size()
        logger.info("Now size is ${size}")
        List<Object> row = sql(checkSQL)[size-1]
        logger.info("Now row is ${row}")

        return (row[3] as String) == "FINISHED"
    }

    Boolean checkRestoreFinish() {
        String checkSQL = "SHOW RESTORE FROM " + context.dbName
        int size = sql(checkSQL).size()
        logger.info("Now size is ${size}")
        List<Object> row = sql(checkSQL)[size-1]
        logger.info("Now row is ${row}")

        return (row[4] as String) == "FINISHED"
    }

    String getServerPrepareJdbcUrl(String jdbcUrl, String database) {
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
        def sql_port
        if (urlWithoutSchema.indexOf("/") >= 0) {
            // e.g: jdbc:mysql://locahost:8080/?a=b
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
        } else {
            // e.g: jdbc:mysql://locahost:8080
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
        }
        // set server side prepared statement url
        return "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + database + "?&useServerPrepStmts=true"
    }

    DebugPoint GetDebugPoint() {
        return debugPoint
    }

<<<<<<< HEAD
    void setFeConfig(String key, Object value) {
        assert key != null
        assert key != ''
        for (def cfg : dorisSelectdbDiffCfgNames) {
            def dorisKey = cfg[0]
            def selectdbKey = cfg[1]
            if (key == dorisKey) {
                if (!context.config.isDorisEnv) {
                    key = selectdbKey
                }
                break
            }

            if (key == selectdbKey) {
                if (context.config.isDorisEnv) {
                    key = dorisKey
                }
                break
            }
        }

        // TODO: apsaradb_env_enabled will be removed
        if (key == 'apsaradb_env_enabled') {
            if (context.config.isDorisEnv) {
                key = 'security_checker_class_name'
                if (value == true || value == 'true') {
                    value = 'com.aliyun.securitysdk.SecurityUtil'
                } else {
                    value = ''
                }
            }
        }

        // not support this key
        if (key == null || key == "") {
            return
        }

=======
    boolean isCloudMode() {
        return !getFeConfig("cloud_unique_id").isEmpty()
    }

    String getFeConfig(String key) {
        return sql_return_maparray("SHOW FRONTEND CONFIG LIKE '${key}'")[0].Value
    }

    void setFeConfig(String key, Object value) {
>>>>>>> b15854a19f
        sql "ADMIN SET FRONTEND CONFIG ('${key}' = '${value}')"
    }

    void setFeConfigTemporary(Map<String, Object> tempConfig, Closure actionSupplier) {
        def oldConfig = tempConfig.keySet().collectEntries { [it, getFeConfig(it)] }

        def updateConfig = { conf ->
            conf.each { key, value -> setFeConfig(key, value) }
        }

        try {
            updateConfig tempConfig
            actionSupplier()
        } finally {
            updateConfig oldConfig
        }
    }
<<<<<<< HEAD

    void waiteCreateTableFinished(String tableName) {
        Thread.sleep(2000);
        String showCreateTable = "SHOW CREATE TABLE ${tableName}"
        String createdTableName = "";
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 1 * 60 * 1000 // 1 min
        do {
            result = sql(showCreateTable)
            if (!result.isEmpty()) {
                createdTableName = result.last().get(0)
            }
            logger.info("create table result of ${showCreateTable} is ${createdTableName}")
            Thread.sleep(500);
        } while (timeoutTimestamp > System.currentTimeMillis() && createdTableName.isEmpty())
        if (createdTableName.isEmpty()) {
            logger.info("create table is not success")
        }
        Assert.assertEquals(true, !createdTableName.isEmpty())
    }

    String[][] deduplicate_tablets(String[][] tablets) {
        def result = [:]

        tablets.each { row ->
            def tablet_id = row[0]
            if (!result.containsKey(tablet_id)) {
                result[tablet_id] = row
            }
        }

        return result.values().toList()
    }

    ArrayList deduplicate_tablets(ArrayList tablets) {
        def result = [:]

        tablets.each { row ->

            def tablet_id
            if (row.containsKey("TabletId")) {
                tablet_id = row.TabletId
            } else {
                tablet_id = row[0]
            }

            if (!result.containsKey(tablet_id)) {
                result[tablet_id] = row
            }
        }

        return result.values().toList()
    }

    def check_mv_rewrite_success = { db, mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains("${mv_name}(${mv_name})")
        }
    }

    def check_mv_rewrite_fail = { db, mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            notContains("${mv_name}(${mv_name})")
        }
    }
=======
>>>>>>> b15854a19f
}


