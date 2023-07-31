/*
 * Copyright 2023 EPAM Systems, Inc
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.deltix.clickhouse.unit;

import com.epam.deltix.clickhouse.util.ExecutorsUtil;
import com.epam.deltix.clickhouse.writer.TableWriter;
import com.epam.deltix.clickhouse.mock.MockPreparedStatement;
import com.epam.deltix.clickhouse.mock.TestModel;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CountFlushingStrategyTests extends BaseFlushingStrategyTests {

    @Test
    public void singleThreadProducerFlushByCountProducesExpectedNumberOfBatches() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int flushSize = 1;
        int messageToSend = 5;

        long expectedBatchCount = flushSize * messageToSend;

        try (TableWriter<TestModel> tableWriter = createTableWriter(messageToSend, flushSize, Long.MAX_VALUE)) {
            sendInParallel(threadPool, 1, messageToSend, tableWriter);

            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        batchStat.forEach(value -> Assert.assertEquals(flushSize, value.longValue()));
    }

    @Test
    public void singleThreadProducerFlushByCountAndFlushWhenClosingProducesExpectedNumberOfBatches() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int flushSize = 5;
        int messageToSend = 16;

        // three flushes by count and one when table writer will closing
        long expectedBatchCount = 4;

        try (TableWriter<TestModel> tableWriter = createTableWriter(messageToSend, flushSize, Long.MAX_VALUE)) {
            sendInParallel(threadPool, 1, messageToSend, tableWriter);

            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        for (int i = 0; i < batchStat.size(); i++) {
            long expectedValue = (i + 1 == expectedBatchCount) ? (messageToSend % flushSize) : flushSize;
            long actualValue = batchStat.get(i);

            Assert.assertEquals(expectedValue, actualValue);
        }
    }

    @Test
    public void multiThreadProducerFlushByTimerProducesExpectedNumberOfBatches() {
        int threadsToRun = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadsToRun);

        int flushSize = 5;
        int messageToSendPerPackage = 100;
        int totalNumberOfMessages = threadsToRun * messageToSendPerPackage;

        long expectedBatchCount = totalNumberOfMessages / flushSize;

        try (TableWriter<TestModel> tableWriter = createTableWriter(totalNumberOfMessages, flushSize, Long.MAX_VALUE)) {

            sendInParallel(threadPool, threadsToRun, messageToSendPerPackage, tableWriter);
            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        batchStat.forEach(value -> Assert.assertEquals(flushSize, value.longValue()));
    }

}