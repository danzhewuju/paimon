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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/5/29
 * @time: 19:11
 */
public class PaimonApiWriteTest {

    @Test
    public void testPaimonWrite() throws Exception {
        Identifier identifier = Identifier.create("paimon_test", "paimon_test");
        Path warehouse = new Path("file" + "://" + "/Users/alex/Program/data/paimon/warehouse");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        org.apache.paimon.table.Table table = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();

        // First record
        Long r1_col1 = Long.valueOf(1);
        BigDecimal bd = new BigDecimal("1.23");
        Decimal r1_col3 =
                org.apache.paimon.data.Decimal.fromBigDecimal(bd, bd.precision(), bd.scale());
        GenericRow record1 = GenericRow.of(r1_col1, r1_col3);
        try {
            write.write(record1, 1); // Using fixed bucket number 1
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Second record
        Long r2_col1 = Long.valueOf(222);
        BigDecimal bd2 = new BigDecimal("333333.3333");
        Decimal r2_col3 =
                org.apache.paimon.data.Decimal.fromBigDecimal(bd2, bd2.precision(), bd2.scale());
        GenericRow record2 = GenericRow.of(r2_col1, r2_col3);
        write.write(record2, 1);

        List<CommitMessage> messages = write.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        System.out.println("done");
    }
}
