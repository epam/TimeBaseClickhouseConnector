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
import com.epam.deltix.clickhouse.writer.IntrospectionType;
import com.epam.deltix.clickhouse.writer.Introspector;
import com.epam.deltix.clickhouse.writer.TableWriter;
import com.epam.deltix.clickhouse.mock.MockPreparedStatement;
import com.epam.deltix.clickhouse.mock.TestModel;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.util.*;
import java.util.concurrent.*;

public class TableWriterTests extends BaseFlushingStrategyTests {

    @Test
    public void singleThreadProducerWritesMessagesInOriginalOrder() {
        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        int flushSize = 10;
        int messageToSend = 10;

        List<Queue<TestModel>> messages;

        try (TableWriter<TestModel> tableWriter = initTableWriter(messageToSend, flushSize, Long.MAX_VALUE)) {
            messages = sendMessagesInParallel(threadPool, 1, messageToSend, tableWriter);

            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        isEveryThreadWriteMessagesInRecordOrder(messages, MockEncoder.models);
    }

    @Test
    public void multiThreadProducerWritesMessagesInOriginalOrder() {
        int threadsToRun = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadsToRun);

        int flushSize = 10;
        int messageToSendPerPackage = 10;

        int totalNumberOfMessages = threadsToRun * messageToSendPerPackage;

        List<Queue<TestModel>> messages;

        try (TableWriter<TestModel> tableWriter = initTableWriter(totalNumberOfMessages, flushSize, Long.MAX_VALUE)) {
            messages = sendMessagesInParallel(threadPool, threadsToRun, messageToSendPerPackage, tableWriter);

            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        isEveryThreadWriteMessagesInRecordOrder(messages, MockEncoder.models);
    }

    @Test
    public void multiThreadProducerWritesMessagesWhenMessageQueueNotBlocked() {
        int threadsToRun = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadsToRun);

        int messageQueueCapacity = 100;

        int packageSize = 100;
        int numberOfPackages = 1;

        // `10` because 10 threads must send by 100 messages with some delay, when a table writer message queue capacity also 100,
        // and each thread must wait for the message queue flush 100 messages and take the next 100.
        long expectedBatchCount = 10;

        try (TableWriter<TestModel> tableWriter = createTableWriter(messageQueueCapacity, Integer.MAX_VALUE, 100)) {
            sendInParallel(threadPool, threadsToRun, packageSize, numberOfPackages, 50, tableWriter);

            ExecutorsUtil.shutdownAndAwaitTermination(threadPool, ExecutorsUtil.DEFAULT_SHUTDOWN_TIMEOUT_MS);
        }

        MockPreparedStatement mockPreparedStatement = mockDataSource.mockConnection.mockPreparedStatement;

        //Assert.assertEquals(expectedBatchCount, mockPreparedStatement.executeBatchCount);

        List<Long> batchStat = mockPreparedStatement.batchStat;

        long sum = batchStat.stream().mapToLong(Long::longValue).sum();

        //batchStat.forEach(value -> Assert.assertEquals(packageSize, value.longValue()));

        Assert.assertEquals(packageSize * numberOfPackages * threadsToRun, sum);
    }


    private List<Queue<TestModel>> sendMessagesInParallel(ExecutorService threadPool, int threadsToRun,
                                                          int messageToSendPerThread, TableWriter<TestModel> tableWriter) {
        List<Queue<TestModel>> result = new LinkedList<>();

        List<Future<Queue<TestModel>>> futures = new LinkedList<>();

        for (int i = 0; i < threadsToRun; i++) {
            final Future<Queue<TestModel>> future = threadPool.submit(new SendMessagesTask(messageToSendPerThread, tableWriter));
            futures.add(future);
        }

        for (Future<Queue<TestModel>> item : futures) {
            try {
                result.add(item.get());
            } catch (InterruptedException | ExecutionException ex) {
                throw new RuntimeException(ex);
            }
        }

        return result;
    }

    private void isEveryThreadWriteMessagesInRecordOrder(List<Queue<TestModel>> messages, List<TestModel> models) {
        for (Queue<TestModel> threadMessages : messages) {
            Iterator<TestModel> modelIterator = models.iterator();

            TestModel threadModel = threadMessages.poll();

            while (modelIterator.hasNext()) {
                TestModel model = modelIterator.next();

                if (model.equals(threadModel)) {
                    modelIterator.remove();
                    threadModel = threadMessages.poll();
                }
            }
        }

        Assert.assertTrue(models.isEmpty());
    }

    private static class SendMessagesTask implements Callable<Queue<TestModel>> {
        private int messageToSendPerThread;
        private TableWriter<TestModel> tableWriter;

        public SendMessagesTask(int messageToSendPerThread, TableWriter<TestModel> tableWriter) {
            this.messageToSendPerThread = messageToSendPerThread;
            this.tableWriter = tableWriter;
        }

        @Override
        public Queue<TestModel> call() {
            Queue<TestModel> threadResult = new LinkedBlockingQueue<>();
            for (int j = 0; j < messageToSendPerThread; j++) {
                TestModel model = new TestModel();

                threadResult.add(model);

                tableWriter.send(model);
            }

            return threadResult;
        }
    }

    private TableWriter<TestModel> initTableWriter(int messageQueueCapacity,
                                                   int flushPackageSize,
                                                   long flushIntervalMs) {
        return new TableWriter<>(mockDataSource,
                Introspector.getTableDeclaration(TestModel.class, IntrospectionType.BY_FIELDS),
                TestModel.class,
                IntrospectionType.BY_FIELDS,
                messageQueueCapacity, flushPackageSize, flushIntervalMs);
    }

    private static class MockEncoder {
        static final List<TestModel> models = new LinkedList<>();

        private PreparedStatement mockUpdateStatement(TestModel model, PreparedStatement statement) {
            models.add(model);

            return statement;
        }
    }
}