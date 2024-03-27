suite("test_show_data") {
  sql """ DROP DATABASE IF EXISTS SHOW_DATA_1; """
  sql """ DROP DATABASE IF EXISTS SHOW_DATA_2; """
  sql """ CREATE DATABASE SHOW_DATA_1; """
  sql """ CREATE DATABASE SHOW_DATA_2; """

  sql """ USE SHOW_DATA_1; """

  sql """ CREATE TABLE `table` (
    `siteid` int(11) NOT NULL COMMENT "",
    `citycode` int(11) NOT NULL COMMENT "",
    `userid` int(11) NOT NULL COMMENT "",
    `pv` int(11) NOT NULL COMMENT ""
  ) ENGINE=OLAP
  DUPLICATE KEY(`siteid`)
  COMMENT "OLAP"
  DISTRIBUTED BY HASH(`siteid`) BUCKETS 1; """

  sql """ insert into `table` values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8); """

  sql """ USE SHOW_DATA_2; """

  sql """ CREATE TABLE `table` (
  `siteid` int(11) NOT NULL COMMENT "",
  `citycode` int(11) NOT NULL COMMENT "",
  `userid` int(11) NOT NULL COMMENT "",
  `pv` int(11) NOT NULL COMMENT ""
  ) ENGINE=OLAP
  DUPLICATE KEY(`siteid`)
  COMMENT "OLAP"
  DISTRIBUTED BY HASH(`siteid`) BUCKETS 1; """

  sql """ insert into `table` values
        (9,10,11,12),
        (9,10,11,12),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16); """
  
  // wait for heartbeat

  long start = System.currentTimeMillis()
  long dataSize = 0
  long current = -1
  do {
    current = System.currentTimeMillis()
    def res = sql """ show data properties("entire_warehouse"="true","db_names"="SHOW_DATA_1"); """
    for (row : res) {
      print row
      if (row[0].toString() == "SHOW_DATA_1") {
        dataSize = row[1].toInteger()
      }
    }
    sleep(1000)
  } while (dataSize == 0 && current - start < 600000)

  qt_show_1 """ show data properties("entire_warehouse"="true","db_names"="SHOW_DATA_1"); """

  qt_show_2 """ show data properties("entire_warehouse"="true","db_names"="SHOW_DATA_2"); """

  qt_show_3 """ show data properties("entire_warehouse"="true","db_names"="SHOW_DATA_1,SHOW_DATA_2"); """

  def result = sql """show data properties("entire_warehouse"="true")"""

  assertTrue(result.size() >= 3)

  sql """ DROP DATABASE IF EXISTS SHOW_DATA_1; """
  result = sql """show data properties("entire_warehouse"="true")"""
  assertTrue(result.size() > 0)
  for (row : result) {
    if (row[0].toString().equalsIgnoreCase("total")) {
      assertTrue(row[2].toInteger() > 0)
    }
  }


}
