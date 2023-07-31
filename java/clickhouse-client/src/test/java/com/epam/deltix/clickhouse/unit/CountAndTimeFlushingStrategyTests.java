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

public class CountAndTimeFlushingStrategyTests extends BaseFlushingStrategyTests {

    @Test
    public void singleThreadProducerFlushByCountProducesExpectedNumberOfBatches() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int flushSize = 5;
        int messageToSend = 100;

        long expectedBatchCount = messageToSend / flushSize;

        try (TableWriter<TestModel> tableWriter = createTableWriter(
                messageToSend, flushSize, Long.MAX_VALUE)) {

            sendInParallel(threadPool, 1, messageToSend, tableWriter);
            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        batchStat.forEach(value -> Assert.assertEquals(flushSize, value.longValue()));
    }

    @Test
    public void singleThreadProducerFlushByTimerProducesExpectedNumberOfBatches() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int packageSize = 1000;
        int numberOfPackages = 5;

        long totalNumberOfMessages = packageSize * numberOfPackages;
        // expected `1` because flush should be by timer only once.
        long expectedBatchCount = 1;

        try (TableWriter<TestModel> tableWriter = createTableWriter(
                (int) totalNumberOfMessages, Integer.MAX_VALUE, 1500)) {

            sendInParallel(threadPool, 1, packageSize, numberOfPackages, 200, tableWriter);
            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        Assert.assertEquals(totalNumberOfMessages, batchStat.get(0).longValue());
    }

    @Test
    public void multiThreadProducerFlushByTimerProducesExpectedNumberOfBatches() {
        int threadsToRun = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadsToRun);

        int packageSize = 100;
        int numberOfPackages = 5;

        long totalNumberOfMessages = threadsToRun * packageSize * numberOfPackages;
        // expected `1` because flush should be by timer only once.
        long expectedBatchCount = 1;

        try (TableWriter<TestModel> tableWriter = createTableWriter(
                (int) totalNumberOfMessages, Integer.MAX_VALUE, 1500)) {

            sendInParallel(threadPool, threadsToRun, packageSize, numberOfPackages, 200, tableWriter);
            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        Assert.assertEquals(totalNumberOfMessages, batchStat.get(0).longValue());
    }

    @Test
    public void singleThreadProducerFlushByCountAndTimerProducesExpectedNumberOfBatches() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int flushSize = 5;
        int messageToSend = 12;

        // two flushes by count and one by timer
        long expectedBatchCount = 3;

        try (TableWriter<TestModel> tableWriter = createTableWriter(
                messageToSend, flushSize, 200)) {

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
}