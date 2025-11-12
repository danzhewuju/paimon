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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @email danyuhao@qq.com
 * @author: Hao Yu
 * @date: 2025/11/10
 * @time: 19:25
 */
public class RandomSelectorRowDataChannelComputer extends RowDataChannelComputer {

    private static final Logger LOG = LoggerFactory.getLogger(RandomSelectorRowDataChannelComputer.class);

    private int randomStartIndex = -1;

    private final long randomSeed = System.nanoTime();

    public RandomSelectorRowDataChannelComputer(TableSchema schema, boolean hasLogSink) {
        super(schema, hasLogSink);
    }

    @Override
    public void setup(int numChannels) {
        super.setup(numChannels);
        // Initialize randomStartIndex here
        if (randomStartIndex == -1) {
            this.randomStartIndex = new Random(randomSeed).nextInt(numChannels);
            LOG.info("Initialized randomStartIndex: {}", randomStartIndex);
        }
    }

    @Override
    public int channel(BinaryRow partition, int bucket) {
        return isHasLogSink()
                ? ChannelComputer.select(bucket, getNumChannels())
                : ChannelComputer.select(partition, bucket, getNumChannels(), randomStartIndex);
    }
}
