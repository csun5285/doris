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
import org.apache.doris.regression.suite.Suite

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;

import java.util.concurrent.TimeUnit;

void checkProcessName(String processName)
{
    if (processName in ["fe", "be", "ms"]) {
        return
    }
    throw new Exception("invalid process name: " + processName)
}

Suite.metaClass.checkClusterDir = {
    if (context.config.clusterDir == null || context.config.clusterDir.isEmpty()) {
        throw new Exception("empty cluster dir")
    }
}

Suite.metaClass.executeCommand = { String nodeIp, String commandStr /* param */ ->
    Suite suite = delegate as Suite

    final SSHClient ssh = new SSHClient()
    ssh.loadKnownHosts()
    ssh.connect(nodeIp)
    Session session = null
    try {
        logger.info("user.name:{}", System.getProperty("user.name"))
        ssh.authPublickey(System.getProperty("user.name"))
        session = ssh.startSession()
        final Command cmd = session.exec(commandStr)
        cmd.join(30, TimeUnit.SECONDS)
        def code = cmd.getExitStatus()
        def out = IOUtils.readFully(cmd.getInputStream()).toString()
        def err = IOUtils.readFully(cmd.getErrorStream()).toString()
        def errMsg = cmd.getExitErrorMessage()
        logger.info("commandStr:${commandStr}")
        logger.info("code:${code}, out:${out}, err:${err}, errMsg:${errMsg}")
        assertEquals(0, code)
    } finally {
        try {
            if (session != null) {
                session.close()
            }
        } catch (IOException e) {
            // Do Nothing   
        }
        ssh.disconnect()
    }
    return
}

Suite.metaClass.stopProcess = { String nodeIp, String processName, String installPath /* param */ ->
    Suite suite = delegate as Suite
    checkProcessName(processName)

    logger.info("stopProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
    String commandStr
    if (processName == "ms") {
        commandStr = "bash -c \"${installPath}/bin/stop.sh\""
    } else {
        commandStr = "bash -c \"${installPath}/bin/stop_${processName}.sh\""
    }

    executeCommand(nodeIp, commandStr)
    return
}

Suite.metaClass.startProcess = { String nodeIp, String processName, String installPath /* param */ ->
    Suite suite = delegate as Suite
    checkProcessName(processName);

    logger.info("startProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}");

    String commandStr
    if (processName == "ms") {
        commandStr = "bash -c \"${installPath}/bin/start.sh  --meta-service --daemon\"";
    } else {
        commandStr = "bash -c \"${installPath}/bin/start_${processName}.sh --daemon\"";
    }

    executeCommand(nodeIp, commandStr)
    return;
}

Suite.metaClass.checkProcessAlive = { String nodeIp, String processName, String installPath /* param */ ->
    Suite suite = delegate as Suite
    logger.info("checkProcessAlive(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
    checkProcessName(processName)

    commandStr = "invalid command"
    if (processName == "fe") {
        commandStr = "bash -c \"ps aux | grep ${installPath}/log/fe.gc.log | grep -v grep\""
    }

    if (processName == "be") {
        commandStr = "bash -c \"ps aux | grep ${installPath}/lib/doris_be | grep -v grep\""
    }

    if (processName == "ms") {
        commandStr = "bash -c \"ps aux | grep '${installPath}/lib/selectdb_cloud --meta-service' | grep -v grep\""
    }

    executeCommand(nodeIp, commandStr)
    return
}

Suite.metaClass.restartProcess = { String nodeIp, String processName, String installPath /* param */ ->
    Suite suite = delegate as Suite
    logger.info("restartProcess(): nodeIp=${nodeIp} installPath=${installPath} processName=${processName}")
    checkProcessName(processName)
    stopProcess(nodeIp, processName, installPath)
    sleep(1000)
    startProcess(nodeIp, processName, installPath)

    int tryTimes = 3
    while (tryTimes-- > 0) {
        try {
            checkProcessAlive(nodeIp, processName, installPath)
            break
        } catch (Exception e) {
            logger.info("checkProcessAlive failed, tryTimes=${tryTimes}")
            sleep(5000)
            if (tryTimes <= 0) {
                throw e
            }
        }
    }
    // sleep 5 seconds for wait qe service ready
    sleep(5000)
}

Suite.metaClass.checkBrokerLoadLoading = { String label /* param */ ->
    // check load state
    int tryTimes = 600
    while (tryTimes-- > 0) {
        def stateResult = sql "show load where Label = '${label}'"
        def loadState = stateResult[stateResult.size() - 1][2].toString()
        if ("pending".equalsIgnoreCase(loadState)) {
            sleep(1000)
            continue
        } 

        logger.info("stateResult:{}", stateResult)
        if ("loading".equalsIgnoreCase(loadState)) {
            break
        }
        if ("cancelled".equalsIgnoreCase(loadState)) {
            throw new IllegalStateException("load ${label} has been cancelled")
        }
        if ("finished".equalsIgnoreCase(loadState)) {
            throw new IllegalStateException("load ${label} has been finished")
        }
    }
}

Suite.metaClass.checkBrokerLoadFinished = { String label /* param */ ->
    // check load state
    int tryTimes = 120
    while (tryTimes-- > 0) {
        def stateResult = sql "show load where Label = '${label}'"
        def loadState = stateResult[stateResult.size() - 1][2].toString()
        if ('cancelled'.equalsIgnoreCase(loadState)) {
            logger.info("stateResult:{}", stateResult)
            throw new IllegalStateException("load ${label} has been cancelled")
        } else if ('finished'.equalsIgnoreCase(loadState)) {
            logger.info("stateResult:{}", stateResult)
            break
        }
        sleep(60000)
    }
}

Suite.metaClass.checkCopyIntoLoading = { String label /* param */ ->
    // check load state
    int tryTimes = 600
    while (tryTimes-- > 0) {
        def stateResult = sql "show copy where label like '${label}'"
        def loadState = stateResult[stateResult.size() - 1][3].toString()
        if ("pending".equalsIgnoreCase(loadState)) {
            sleep(1000)
            continue
        } 

        logger.info("stateResult:{}", stateResult)
        if ("loading".equalsIgnoreCase(loadState)) {
            break
        }
        if ("cancelled".equalsIgnoreCase(loadState)) {
            throw new IllegalStateException("copy into ${label} has been cancelled")
        }
        if ("finished".equalsIgnoreCase(loadState)) {
            throw new IllegalStateException("copy into ${label} has been finished")
        }
    }
}

Suite.metaClass.checkCopyIntoFinished = { String label /* param */ ->
    // check load state
    int tryTimes = 120
    while (tryTimes-- > 0) {
        def stateResult = sql "show copy where label like '${label}'"
        def loadState = stateResult[stateResult.size() - 1][3].toString()
        if ('cancelled'.equalsIgnoreCase(loadState)) {
            logger.info("stateResult:{}", stateResult)
            throw new IllegalStateException("copy into ${label} has been cancelled")
        } else if ('finished'.equalsIgnoreCase(loadState)) {
            logger.info("stateResult:{}", stateResult)
            break
        }
        sleep(60000)
    }
}



