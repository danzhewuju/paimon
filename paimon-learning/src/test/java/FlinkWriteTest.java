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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.Test;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2024/11/21
 * @time: 20:32
 */
public class FlinkWriteTest {

    public static void main(String[] args) {
        FlinkWriteTest flinkWriteTest = new FlinkWriteTest();
        flinkWriteTest.testPaimonWrite();
    }

    @Test
    public void testPaimonWrite() {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        configuration.setString("execution.runtime-mode", "streaming");
        // checkpoint 配置
        configuration.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        configuration.setLong("execution.checkpointing.interval", 10000l);
        configuration.setString("execution.checkpointing.timeout", "10min");
        configuration.setString("execution.checkpointing.min-pause", "1min");
        configuration.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        configuration.setString("execution.checkpointing.tolerable-failed-checkpoints", "100000");
        configuration.setString("execution.checkpointing.max-concurrent-checkpoints", "1");
        configuration.setString("state.checkpoints.dir", "hdfs://localhost:9000/flink/checkpoints");
        configuration.setString("state.savepoints.dir", "hdfs://localhost:9000/flink/savepoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(10000l);
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tableEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                " 'type'='paimon',\n" +
                " 'warehouse'='file:///Users/alex/Program/data/paimon/warehouse'\n" +
                ")");

        tableEnv.executeSql("use catalog hadoop_catalog");
        tableEnv.executeSql("use paimon_test");

        // paimon====================================================================
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS paimon_user(\n" +
                " id bigint, " +
                " username STRING, " +
                " password STRING, " +
                " email STRING, " +
                " created_at STRING,\n" +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                " 'connector' = 'paimon'" +
                ")\n");

        tableEnv.executeSql("CREATE TEMPORARY TABLE source (\n" +
                " id int PRIMARY KEY NOT ENFORCED,\n" +
                " name STRING,\n" +
                " password STRING,\n" +
                " email STRING,\n" +
                " created_at as PROCTIME()\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen', \n" +
                " 'fields.id.kind' = 'sequence', " +
                " 'fields.id.start' = '-2', " +
                " 'fields.id.end' = '1000000', " +
                " 'rows-per-second' = '1' \n" +
                ")");
        tableEnv.executeSql("insert into paimon_user select id, name, password, email, cast(created_at as String) from source ");
    }

}
