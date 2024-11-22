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

package org.apache.paimon.flink;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/6/25
 * @time: 19:59
 */
public class PaimonDebugITCaseTest extends CatalogITCaseBase {

    private static final String PAIMON_PATH =
            "/Users/alex/Program/data/paimon/warehouse/default.db";

    @Override
    public void before() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setString("heartbeat.timeout", "300000");
        super.tEnv = tableEnvironmentBuilder().batchMode().setConf(configuration).build();
        String catalog = "PAIMON";
        path = PAIMON_PATH;
        System.out.println("Tmp path: " + path);
        String inferScan =
                !inferScanParallelism() ? ",\n'table-default.scan.infer-parallelism'='false'" : "";

        Map<String, String> options = new HashMap<>(catalogOptions());
        options.put("type", "paimon");
        options.put("warehouse", toWarehouse(path));
        super.tEnv.executeSql(
                String.format(
                        "CREATE CATALOG %s WITH (" + "%s" + inferScan + ")",
                        catalog,
                        options.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(","))));
        super.tEnv.useCatalog(catalog);

        super.sEnv = tableEnvironmentBuilder().streamingMode().checkpointIntervalMs(100).build();
        super.sEnv.registerCatalog(catalog, tEnv.getCatalog(catalog).get());
        super.sEnv.useCatalog(catalog);

        setParallelism(defaultParallelism());
        prepareEnv();
    }

    @Test
    public void testSteamingJob()
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        before();
        setParallelism(1);
        sEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS T ("
                        + "pt INT, "
                        + "pk INT, "
                        + "v STRING, "
                        + "PRIMARY KEY (pt, pk) NOT ENFORCED"
                        + ") PARTITIONED BY (pk) WITH ("
                        + " 'bucket'='-1', "
                        + " 'dynamic-bucket.target-row-num'='3' "
                        + ")");
        sEnv.executeSql(
                "Create TEMPORARY table source (\n"
                        + "    id int,\n"
                        + "    ts int,\n"
                        + "    name string,\n"
                        + "    primary key (id) not enforced\n"
                        + ") with (\n"
                        + "    'connector' = 'datagen',\n"
                        + "    'rows-per-second' = '1',\n"
                        + "     'fields.ts.min' = '0',\n"
                        + "     'fields.ts.max' = '10'\n"
                        + ")");

        sEnv.executeSql("INSERT INTO T SELECT * FROM source")
                .await(10 * 60 * 1000, TimeUnit.SECONDS);
    }
}
