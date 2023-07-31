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

import com.epam.deltix.clickhouse.writer.IntrospectionType;
import com.epam.deltix.clickhouse.writer.Introspector;
import com.epam.deltix.clickhouse.writer.TableWriter;
import com.epam.deltix.clickhouse.mock.MockDataSource;
import com.epam.deltix.clickhouse.mock.TestModel;
import org.junit.Before;

import java.sql.PreparedStatement;
import java.util.concurrent.ExecutorService;

public class BaseFlushingStrategyTests {

    protected MockDataSource mockDataSource;

    @Before
    public void init() {
        mockDataSource = new MockDataSource();
    }

    protected void sendInParallel(ExecutorService threadPool, int threadsToRun,
                                  int messageToSendPerPackage, TableWriter<TestModel> tableWriter) {
        sendInParallel(threadPool, threadsToRun, messageToSendPerPackage, 1, Long.MIN_VALUE, tableWriter);
    }

    protected void sendInParallel(ExecutorService threadPool, int threadsToRun, int messageToSendPerPackage,
                                  int packageCount, TableWriter<TestModel> tableWriter) {
        sendInParallel(threadPool, threadsToRun, messageToSendPerPackage, packageCount, Long.MIN_VALUE, tableWriter);
    }

    protected void sendInParallel(ExecutorService threadPool, int threadsToRun,
                                  int messageToSendPerPackage, int packageCount,
                                  long sendingDelayBetweenPackagesMs, TableWriter<TestModel> tableWriter) {
        for (int i = 0; i < threadsToRun; i++) {
            threadPool.execute(() -> {
                for (int p = 0; p < packageCount; p++) {
                    if (sendingDelayBetweenPackagesMs != Long.MIN_VALUE) {
                        sleep(sendingDelayBetweenPackagesMs);
                    }

                    for (int j = 0; j < messageToSendPerPackage; j++) {
                        TestModel model = new TestModel();

                        tableWriter.send(model);
                    }
                }
            });
        }
    }


    protected TableWriter<TestModel> createTableWriter(int messageQueueCapacity,
                                                       int flushPackageSize,
                                                       long flushIntervalMs) {
        return new TableWriter<>(mockDataSource,
                Introspector.getTableDeclaration(TestModel.class, IntrospectionType.BY_FIELDS),
                TestModel.class,
                IntrospectionType.BY_FIELDS,
                messageQueueCapacity, flushPackageSize, flushIntervalMs);
    }


    protected static PreparedStatement updateStatement(TestModel model, PreparedStatement statement) {
        return statement;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}