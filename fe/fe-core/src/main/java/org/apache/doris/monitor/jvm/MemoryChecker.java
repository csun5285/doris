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

package org.apache.doris.monitor.jvm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * physical memory should > xmx + direct + 1GB
 */
public class MemoryChecker {
    private static final Logger LOG = LogManager.getLogger(MemoryChecker.class);

    /**
     * physical memory should > xmx + direct + 1GB
     */
    public static void check() throws InvocationTargetException {
        // for xmx
        long maxMemory = Runtime.getRuntime().maxMemory();

        // for direct
        long maxDirectMemory = 0L;
        java.lang.management.RuntimeMXBean runtimemxBean = java.lang.management.ManagementFactory.getRuntimeMXBean();
        java.util.List<String> arguments = runtimemxBean.getInputArguments();
        for (String s : arguments) {
            if (s.startsWith("-XX:MaxDirectMemorySize=")) {
                String memSize = s.split("=")[1];
                if (memSize.endsWith("k")) {
                    maxDirectMemory = Long.parseLong(memSize.substring(0, memSize.length() - 1))
                            * 1024L;
                } else if (memSize.endsWith("m")) {
                    maxDirectMemory = Long.parseLong(memSize.substring(0, memSize.length() - 1))
                            * 1024L * 1024L;
                } else if (memSize.endsWith("g")) {
                    maxDirectMemory = Long.parseLong(memSize.substring(0, memSize.length() - 1))
                            * 1024L * 1024L * 1024L;
                } else {
                    maxDirectMemory = Long.parseLong(memSize);
                }
            }
        }
        if (maxDirectMemory == 0L) {
            // if maxDirectMemorySize is not specified，then it will be the max heap memory
            maxDirectMemory = Runtime.getRuntime().maxMemory();
        }

        LOG.info("xmx is " + String.valueOf(maxMemory));
        LOG.info("max direct memory is " + String.valueOf(maxDirectMemory));

        // for physical memory limit
        String os = System.getProperty("os.name").toLowerCase();
        Long physicalMemoryLimit = 0L;
        if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
            // the execution platform is Linux
            Long[] physicalMemroy = new Long[1];
            readPhysicalMermory(physicalMemroy);
            Long[] cGroupMemLimit = new Long[1];
            boolean findMemLimitSuccess = findCGroupMemLimit(cGroupMemLimit);
            physicalMemoryLimit = physicalMemroy[0];
            if (findMemLimitSuccess && cGroupMemLimit[0] > 0) {
                physicalMemoryLimit = Math.min(cGroupMemLimit[0], physicalMemoryLimit);
            }

            LOG.info("physical memory from /proc/meminfo is " + String.valueOf(physicalMemroy[0]));
            LOG.info("CGroupMemLimit is " + String.valueOf(cGroupMemLimit[0]));
        } else {
            // Other platforms
            SystemInfo si = new SystemInfo();
            HardwareAbstractionLayer hal = si.getHardware();
            physicalMemoryLimit = hal.getMemory().getTotal();
        }
        LOG.info("physical memory limit is " + String.valueOf(physicalMemoryLimit));

        // check physical memroy >= xmx + direct + oneGB
        long oneGB = 1024L * 1024L * 1024L;
        if (physicalMemoryLimit < maxDirectMemory + maxMemory + oneGB) {
            LOG.warn("physical memory < xmx + direct + 1GB");
            System.err.println("physical memory < xmx + direct + 1GB");
            throw new IllegalArgumentException("Physical memory < xmx + direct + 1GB");
        }
    }

    /* *
     *  read physical memomry size from /proc/meminfo
     *
     *  @param[out] bytes: byte[0] stores the value of physical memory size
     *  @return: whether read success
     */
    public static boolean readPhysicalMermory(Long[] bytes) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/meminfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\s+");
                if (fields.length < 2) {
                    continue;
                }

                String key = fields[0].substring(0, fields[0].length() - 1);
                if (key.equals("MemTotal")) {
                    String valueStr = fields[1];
                    Long memValue = Long.valueOf(valueStr);
                    if (fields.length == 2) {
                        bytes[0] = memValue;
                    } else if (fields[2].equals("kB")) {
                        bytes[0] = memValue * 1024L;
                    }
                    return true;
                }
            }
            LOG.warn("can find the MemTotal in /proc/meminfo");
            return false;
        } catch (IOException e) {
            // Handle the exception as needed
            LOG.warn("can't read /proc/mrminfo file for physical memory size");
            return false;
        } catch (NumberFormatException e) {
            LOG.warn("parse the physical memory size fail");
            return false;
        }
    }

    /* *
     *  for a subsystem, like 'memory', in CGroup, find the absolute path
     *
     *  @param subsytem: the name of the subsystem we want to find path
     *  @param[out] path: the return path of the subsystem
     *  @return: whether execution success
     */
    public static boolean findAbsCGroupPath(String subsystem, StringBuilder path) {
        boolean findGlobalCGroupPathSuccess = findGlobalCGroup(subsystem, path);

        List<String> paths = new ArrayList<String>();
        boolean findCGroupMountsSucces = findCGroupMounts(subsystem, paths);
        if (!findGlobalCGroupPathSuccess || !findCGroupMountsSucces) {
            return false;
        }

        String mountPath = paths.get(0);
        String systemPath = paths.get(1);

        if (!path.substring(0, systemPath.length()).equals(systemPath)) {
            LOG.warn(String.format("Expected CGroup path '%s' to start with '%s'", path.toString(), systemPath));
            return false;
        }

        path.replace(0, systemPath.length(), mountPath);
        return true;
    }

    /* *
     *  for a subsystem, like 'memory', in CGroup, find the global path
     *  absolute path is depended on the global path and mount path
     *  the detail can be found in method 'findAbsCGroupPath'
     *
     *  @param subsytem: the name of the subsystem we want to find path
     *  @param[out] path: the return global path of the subsystem
     *  @return: whether execution success
     */
    public static boolean findGlobalCGroup(String subsystem, StringBuilder path) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/self/cgroup"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(":");
                if (fields.length != 3) {
                    LOG.warn(String.format("Could not parse line from /proc/self/cgroup - had %d > 3 tokens: '%s'",
                            fields.length, line));
                    return false;
                }
                String[] subsystems = fields[1].split(",");
                for (String sub : subsystems) {
                    if (sub.equals(subsystem)) {
                        path.setLength(0);
                        path.append(fields[2]);
                        return true;
                    }
                }
            }
            LOG.warn(String.format("Could not find subsystem %s in /proc/self/cgroup", subsystem));
            return false;
        } catch (IOException e) {
            LOG.warn(String.format("Error reading /proc/self/cgroup: %s", e.getMessage()));
            return false;
        }
    }

    /* *
     *  for a subsystem, like 'memory', in CGroup, find the mount path
     *  absolute path is depended on the global path and mount path
     *  the detail can be found in method 'findAbsCGroupPath'
     *
     *  @param subsytem: the name of the subsystem we want to find path
     *  @param[out] path: the return mount path of the subsystem
     *  @return: whether execution success
     */
    public static boolean findCGroupMounts(String subsystem, List<String> paths) {
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/self/mountinfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(" ");
                if (fields.length < 7) {
                    LOG.warn(String.format("Could not parse line from /proc/self/mountinfo - had %d > 7 tokens: '%s'",
                            fields.length, line));
                    return false;
                }
                if (!fields[fields.length - 3].equals("cgroup")) {
                    continue;
                }
                // List<String> cgroupOpts = splitFields(fields[fields.length - 1], ",");
                List<String> cgroupOpts = Arrays.asList(fields[fields.length - 1].split(","));
                if (!cgroupOpts.contains(subsystem)) {
                    continue;
                }
                String mountPath = unescapeString(fields[4]);
                String systemPath = unescapeString(fields[3]);
                if (systemPath.endsWith("/")) {
                    systemPath = systemPath.substring(0, systemPath.length() - 1);
                }
                paths.add(mountPath);
                paths.add(systemPath);
                return true;
            }
            LOG.warn(String.format("Could not find subsystem %s in /proc/self/mountinfo", subsystem));
            return false;
        } catch (IOException e) {
            LOG.warn("Error reading /proc/self/mountinfo: " + e.getMessage());
            return false;
        }
    }

    private static String unescapeString(String input) {
        Pattern pattern = Pattern.compile("\\\\([\\\\\"'ntbrf]|u[0-9a-fA-F]{4})");
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement;
            if (matcher.group(1).startsWith("u")) {
                char unicodeChar = (char) Integer.parseInt(matcher.group(1).substring(1), 16);
                replacement = String.valueOf(unicodeChar);
            } else {
                replacement = getReplacement(matcher.group(1));
            }
            matcher.appendReplacement(result, "");
            result.append(replacement);
        }
        matcher.appendTail(result);

        return result.toString();
    }

    private static String getReplacement(String escaped) {
        switch (escaped) {
            case "\\\\": return "\\";
            case "\\\"": return "\"";
            case "\\'": return "'";
            case "\\n": return "\n";
            case "\\t": return "\t";
            case "\\b": return "\b";
            case "\\r": return "\r";
            case "\\f": return "\f";
            default: return escaped;
        }
    }

    /* *
     *  read CGroup value
     *
     *  @param[out] val: val[0] is the val of the CGroup value
     *  @return: whether read success
     */
    public static boolean readCGroupValue(String limitFilePath, Long[] val) {
        try (BufferedReader reader = new BufferedReader(new FileReader(limitFilePath))) {
            String line = reader.readLine();
            if (line == null) {
                LOG.warn(String.format("Error reading %s: Empty file", limitFilePath));
                return false;
            }

            try {
                val[0] = Long.valueOf(line);
            } catch (NumberFormatException e) {
                LOG.warn(String.format("Failed to parse %s as long: '%s'", limitFilePath, line));
                return false;
            }

            return true;
        } catch (IOException e) {
            LOG.warn(String.format("Error reading %s: %s", limitFilePath, e.getMessage()));
            return false;
        }
    }

    /* *
     *  read CGroup Memory Limit
     *
     *  @param[out] bytes: bytes[0] is the val of memory limit
     *  @return: whether exectuion success
     */
    public static boolean findCGroupMemLimit(Long[] bytes) {
        StringBuilder cgroupPath = new StringBuilder();
        if (!findAbsCGroupPath("memory", cgroupPath)) {
            return false;
        }

        String limitFilePath = cgroupPath.toString() + "/memory.limit_in_bytes";
        return readCGroupValue(limitFilePath, bytes);
    }
}
