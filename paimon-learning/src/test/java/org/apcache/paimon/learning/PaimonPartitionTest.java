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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/12/8
 * @time: 17:36
 */
public class PaimonPartitionTest {

    @Test
    public void testPaimonPartition() throws InterruptedException {
        String paimonWarehouse = "file" + "://" + "/Users/alex/Program/data/paimon/warehouse";
        TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        tableEnv.executeSql(
                "CREATE CATALOG hadoop_catalog WITH (\n"
                        + " 'type'='paimon',\n"
                        + String.format(" 'warehouse'='%s'\n", paimonWarehouse)
                        + ")");

        tableEnv.executeSql("use catalog hadoop_catalog");
        String sql =
                "CREATE TABLE IF NOT EXISTS paimon_partition_test (\n"
                        + "user_id BIGINT,\n"
                        + "item_id BIGINT,\n"
                        + "behavior STRING,\n"
                        + "dt STRING,\n"
                        + "hh STRING,\n"
                        + "scene STRING\n"
                        + ") PARTITIONED BY (dt, hh, scene);\n";

        tableEnv.executeSql("create database if not exists paimon_test");
        tableEnv.executeSql("use paimon_test");
        tableEnv.executeSql(sql);
        tableEnv.executeSql(
                        "INSERT INTO paimon_partition_test VALUES (1, 1, 'click', '2025-12-01', '01', 'ADS_CTRIP_INLAND_RECOMMEND_SCENE');")
                .collect();
        tableEnv.executeSql(
                        "INSERT INTO paimon_partition_test VALUES (2, 2, 'click', '2025-12-08', '01', 'ADS_CTRIP_INLAND_RECOMMEND_SCENE');")
                .collect();
        tableEnv.executeSql(
                        "INSERT INTO paimon_partition_test VALUES (3, 3, 'click', '2025-12-08', '01', 'ADS_CTRIP_INLAND_RECOMMEND_SCENE');")
                .collect();
        //        tableEnv.executeSql(
        //                "CALL sys.expire_partitions(`table` =>
        // 'paimon_test.paimon_partition_test', expiration_time => '3 d', expire_strategy =>
        // 'values-time');");

        TableResult result = tableEnv.executeSql("SELECT * FROM paimon_partition_test");

        result.print();
    }
}
