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

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ListTopicsOptions

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

suite("test_routine_load", "external,external_docker") {
    def topic = "test-topic"

    String kafka_broker_list = context.config.otherConfigs.get("externalEnvIp") + ":" + context.config.otherConfigs.get("kafka_port")
    ExecutorService pool;
    pool = Executors.newFixedThreadPool(1)
    pool.execute{
         def props = new Properties()
         props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_broker_list)
         props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
         'org.apache.kafka.common.serialization.StringSerializer')
         props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
         'org.apache.kafka.common.serialization.StringSerializer')

         AdminClient adminClient = AdminClient.create(props);
         def delResult = adminClient.deleteTopics([topic] as List<String>)
         println("the result is " + delResult);
         def cnt = 0
         while (!delResult.all().isDone()) {
             sleep(1000)
             if (cnt++ > 100) {
                log.info("failed to wait for delResult")
                break
             }
         }

         NewTopic newTopic = new NewTopic(topic, 10, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)
         List<NewTopic> newTopics = new ArrayList<NewTopic>();
         newTopics.add(newTopic);
         def result = adminClient.createTopics(newTopics);
         println("the result is " + result);

         adminClient.close();

         def producer = new KafkaProducer<String, String>(props)
         for (int i = 0; i < 10; i++) {
             String msg_key = i.toString();
             String msg_value = i.toString() + "|" + "abc" + "|" + (i * 2).toString();
             def message = new ProducerRecord<String, String>(topic, msg_key, msg_value)
             producer.send(message)
         }

         sleep(30000)

         for (int i = 10; i < 20; i++) {
             String msg_key = i.toString();
             String msg_value = i.toString() + "|" + "abc" + "|" + (i * 2).toString();
             def message = new ProducerRecord<String, String>(topic, msg_key, msg_value)
             producer.send(message)
         }

         sleep(30000)

         for (int i = 20; i < 30; i++) {
             String msg_key = i.toString();
             String msg_value = i.toString() + "|" + "abc" + "|" + (i * 2).toString();
             def message = new ProducerRecord<String, String>(topic, msg_key, msg_value)
             producer.send(message)
         }

         producer.close()
    }

    pool.shutdown()                 //all tasks submitted

    def tableName = "test_routine_load"
    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id INT,
            name CHAR(10),
            score INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1;
    """

    sleep(1000);
    long timestamp = System.currentTimeMillis()
    String job_name = "routine_load_test_" + String.valueOf(timestamp);
    sql """
        CREATE ROUTINE LOAD ${job_name} ON
        ${tableName} COLUMNS TERMINATED BY "|",
        COLUMNS(id, name, score)
        PROPERTIES(
        "desired_concurrent_number"="2",
        "max_batch_interval"="6",
        "max_batch_rows"="200000",
        "max_batch_size"="104857600")
        FROM KAFKA(
        "kafka_broker_list"="${kafka_broker_list}",
        "kafka_topic"="${topic}",
        "property.group.id"="gid6",
        "property.clinet.id"="cid6",
        "property.kafka_default_offsets"="OFFSET_BEGINNING");
    """

    sleep(50000);
    order_qt_q1 "select * from ${tableName}"

    sql """ PAUSE ROUTINE LOAD FOR ${job_name} """
    sleep(40000);
    order_qt_q2 "select * from ${tableName}"

    sql """ ALTER ROUTINE LOAD FOR ${job_name} PROPERTIES ("max_error_number" = "1") """
    sleep(2);

    sql """ ALTER ROUTINE LOAD FOR ${job_name} PROPERTIES ("max_error_number" = "0") """
    sleep(2);

    sql """ RESUME ROUTINE LOAD FOR ${job_name} """
    sleep(20000);

    while (!pool.isTerminated()){}
    sleep(7000);
    order_qt_q3 "select * from ${tableName}"
}
