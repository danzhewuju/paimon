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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/2/20
 * @time: 14:48
 */
public class FlinkReadTest {
    public static void main(String[] args) {
        FlinkReadTest flinkReadTest = new FlinkReadTest();
        System.out.println("开始运行~");
        flinkReadTest.testPaimonRead();
    }

    @Test
    public void testPaimonRead() {

        String paimonWarehouse =
                "file" + "://" + "/Users/alex/Program/data/paimon/warehouse";
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql(
                "CREATE CATALOG hadoop_catalog WITH (\n"
                        + " 'type'='paimon',\n"
                        + String.format(" 'warehouse'='%s'\n", paimonWarehouse)
                        + ")");

        tableEnv.executeSql("use catalog hadoop_catalog");
        tableEnv.executeSql("create database if not exists paimon_test");
        tableEnv.executeSql("use paimon_test");

        // paimon====================================================================
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS paimon_user(\n"
                        + " id bigint, "
                        + " username STRING, "
                        + " password STRING, "
                        + " email STRING, "
                        + " created_at STRING,\n"
                        + "PRIMARY KEY (id) NOT ENFORCED"
                        + ") with ("
                        + " 'connector' = 'paimon'"
                        + ")\n");

        tableEnv.executeSql("insert into paimon_user values (1, 'alex', '123456', 'alex@163.com', '2023-02-20 14:48:00')").collect();

        tableEnv.executeSql("insert into paimon_user values (2, 'hao', '123456', 'hao@163.com', '2023-02-20 14:48:00')").collect();

        TableResult tableResult = tableEnv.executeSql("select * from paimon_user;");
        tableResult.print();

        tableEnv.executeSql("delete from paimon_user where id = 1").collect();

        tableResult = tableEnv.executeSql("select * from paimon_user;");
        tableResult.print();

        // compact
//        CloseableIterator<Row> collect = tableEnv.executeSql("CALL sys.compact(`table` => 'paimon_test.paimon_user')").collect();
//        while (collect.hasNext()) {
//            System.out.println(collect.next());
//        }

    }
}
