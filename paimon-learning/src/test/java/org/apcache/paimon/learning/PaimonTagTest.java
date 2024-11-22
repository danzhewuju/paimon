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

package org.apcache.paimon.learning;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/11/12
 * @time: 10:49
 */
public class PaimonTagTest {

    @Test
    public void testPaimonTest() throws InterruptedException {
        String paimonWarehouse = "file" + "://" + "/Users/alex/Program/data/paimon/warehouse";

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
//        configuration.setString("execution.runtime-mode", "streaming");
        configuration.setString("execution.runtime-mode", "batch");
        // checkpoint 配置
        configuration.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        configuration.setLong("execution.checkpointing.interval", 30000L);
        configuration.setString("execution.checkpointing.timeout", "1min");
        configuration.setString("execution.checkpointing.min-pause", "10s");
        configuration.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");
        configuration.setString("execution.checkpointing.tolerable-failed-checkpoints", "100000");
        configuration.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
        configuration.setString(
                "state.checkpoints.dir", "file:/Users/alex/Program/data/flink/checkpoint");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(10);
        env.enableCheckpointing(10000L);
        TableEnvironment tableEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());


        tableEnv.executeSql(
                "CREATE CATALOG hadoop_catalog WITH (\n"
                        + " 'type'='paimon',\n"
                        + String.format(" 'warehouse'='%s'\n", paimonWarehouse)
                        + ")");

        tableEnv.executeSql("use catalog hadoop_catalog");
        tableEnv.executeSql("create database if not exists paimon_test");
        tableEnv.executeSql("use paimon_test");
        tableEnv.executeSql("CALL sys.create_tag(`table` => 'paimon_test.paimon_user', `tag` => 'test_v7', `time_retained` => '7 d')");
        System.out.println("create tag success");


//
//        // paimon====================================================================
//        tableEnv.executeSql(
//                "CREATE TABLE IF NOT EXISTS paimon_user(\n"
//                        + " id bigint, "
//                        + " username STRING, "
//                        + " password STRING, "
//                        + " email STRING, "
//                        + " created_at STRING,\n"
//                        + "PRIMARY KEY (id) NOT ENFORCED"
//                        + ") with ("
//                        + " 'connector' = 'paimon'"
//                        + ")\n");
//
//        tableEnv.executeSql(
//                "CREATE TEMPORARY TABLE source (\n"
//                        + " id int PRIMARY KEY NOT ENFORCED,\n"
//                        + " name STRING,\n"
//                        + " password STRING,\n"
//                        + " email STRING,\n"
//                        + " created_at as PROCTIME()\n"
//                        + ") WITH (\n"
//                        + " 'connector' = 'datagen', \n"
//                        + " 'fields.id.kind' = 'sequence', "
//                        + " 'fields.id.start' = '-2', "
//                        + " 'fields.id.end' = '1000000', "
//                        + " 'rows-per-second' = '10' \n"
//                        + ")");
//        tableEnv.executeSql(
//                "insert into paimon_user select id, name, password, email, cast(created_at as String) from source ");
//        Thread.currentThread().join();
    }
}
