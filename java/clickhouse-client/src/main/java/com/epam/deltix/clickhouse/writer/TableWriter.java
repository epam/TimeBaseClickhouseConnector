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
package com.epam.deltix.clickhouse.writer;

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.ClickhouseClientSettings;
import com.epam.deltix.clickhouse.models.BindDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.util.*;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogEntry;
import com.epam.deltix.gflog.api.LogFactory;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.Implementation;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.bytebuddy.matcher.ElementMatchers.*;

public class TableWriter<T> implements Closeable {

    private class Flusher implements Runnable {
        private final AutoResetEvent autoResetEvent = new AutoResetEvent(false);
        private long lastExecuteTimestamp = System.currentTimeMillis();

        @Override
        public void run() {
            while (!TableWriter.this.inClosing.get()) {
                try {
                    autoResetEvent.waitOne(TableWriter.this.flushIntervalMs);

                    int queueSize = TableWriter.this.messages.size();

                    // flush if
                    if (queueSize < TableWriter.this.flushSize && // package size is reached
                            (queueSize == 0 || System.currentTimeMillis() - lastExecuteTimestamp < TableWriter.this.flushIntervalMs) && // or time interval is reached and queue is not empty
                            (queueSize == 0 || !TableWriter.this.inClosing.get())) // or we are closing and queue is not empty
                        continue; // nothing to flush, wait next cycle

                    TableWriter.LOG.debug()
                            .append("Start flushing procedure. ")
                            .append(queueSize)
                            .append(" messages in queue.")
                            .commit();

//                    clickhouseClient.executeInSqlConnection(connection -> {
                    try (Connection connection = clickhouseClient.getConnection()) {
                        try (PreparedStatement statement = connection.prepareStatement(insertIntoQuery)) {

                            int batchMsgCount = 0;

                            while (true) {
                                T message = messages.poll();

                                if (message == null) { // queue empty
                                    if (batchMsgCount > 0) { // write last chunk
                                        TableWriter.LOG.debug()
                                                .append("Flushing package of ")
                                                .append(batchMsgCount)
                                                .append(" messages.")
                                                .commit();

                                        statement.executeBatch();
                                    }

                                    TableWriter.LOG.debug()
                                            .append("Finish flushing procedure.")
                                            .commit();

                                    lastExecuteTimestamp = System.currentTimeMillis();
                                    break;
                                }

                                encoder.encode(message, statement);
                                statement.addBatch();
                                batchMsgCount++;

                                if (batchMsgCount == TableWriter.this.flushSize) {
                                    TableWriter.LOG.debug()
                                            .append("Flushing package of ")
                                            .append(batchMsgCount)
                                            .append(" messages.")
                                            .commit();

                                    statement.executeBatch();

                                    // continue to write only if we are closing or have a complete package to write
                                    // otherwise wait for new data to come
                                    if (!TableWriter.this.inClosing.get() &&
                                            TableWriter.this.messages.size() < TableWriter.this.flushSize) {
                                        TableWriter.LOG.debug()
                                                .append("Finish flushing procedure.")
                                                .commit();

                                        lastExecuteTimestamp = System.currentTimeMillis();
                                        break;
                                    }

                                    batchMsgCount = 0;
                                }
                            }
                        }
                    }
                    //});
                } catch (Exception e) {
                    TableWriter.LOG
                            .error()
                            .append("Exception in flusher thread.")
                            .append(System.lineSeparator())
                            .append(e)
                            .commit();
                }
            }

            TableWriter.LOG
                    .info()
                    .append("Flusher thread finished.")
                    .commit();
        }
    }


    private static final Log LOG = LogFactory.getLog(TableWriter.class);

    private static final UUID WRITER_ID = UUID.randomUUID();

    private final ClickhouseClient clickhouseClient;
    private final ClickhouseClientSettings settings;
    private final BlockingQueue<T> messages;
    private final Flusher flusher = new Flusher();
    private final Thread flusherThread;
    private final AtomicBoolean inClosing = new AtomicBoolean();
    private Encoder encoder;
    private String insertIntoQuery;
    private TableDeclaration declaration;
    private int flushSize;
    private long flushIntervalMs;


    public TableWriter(DataSource dataSource,
                       TableDeclaration declaration,
                       Class<T> sourceClass,
                       IntrospectionType introspectionType,
                       int messageQueueCapacity,
                       int flushSize,
                       long flushIntervalMs) throws IllegalStateException {
        debug()
                .append("initializing.")
                .commit();

        this.flushSize = flushSize;
        this.flushIntervalMs = flushIntervalMs;

        messages = new ArrayBlockingQueue<>(messageQueueCapacity);
        clickhouseClient = new ClickhouseClient(dataSource);
        settings = clickhouseClient.getSettings();
        this.declaration = declaration;

        List<BindDeclaration> binds = BindHelper.getBindDeclarations(sourceClass, declaration, introspectionType);
        try {
            this.encoder = getWriteCodec(sourceClass, binds, introspectionType).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            error()
                    .append("Error while creating generated class.")
                    .append(e)
                    .commit();
            throw new IllegalStateException();
        }

        insertIntoQuery = SqlQueryHelper.getInsertIntoQuery(declaration.getTableIdentity(), binds);

        flusherThread = new Thread(flusher, "TableWriter flusher thread");
        flusherThread.start();
    }


    private Class<? extends Encoder> getWriteCodec(Class<T> sourceClass, List<BindDeclaration> binds, IntrospectionType introspectionType) {
        DynamicType.Unloaded<? extends Encoder> unloadedType = new ByteBuddy()
                .subclass(Encoder.class)
                .name("WriteCodec")
                .method(named("encode"))
                .intercept(new Implementation.Simple(new WriterCodecGenerator(sourceClass, binds, introspectionType)))
                .make();

        if (settings.isSaveGeneratedClasses())
            ByteBuddyHelper.saveClass(unloadedType, settings.getGeneratedClassPath());

        Class<? extends Encoder> dynamicType = unloadedType
                .load(Encoder.class.getClassLoader())
                .getLoaded();

        return dynamicType;
    }

    public final synchronized void createTable(boolean createDatabase, boolean createIfNotExists) throws SQLException {
        if (createDatabase) {
            debug()
                    .append("Creating database ")
                    .append(declaration.getTableIdentity().getDatabaseName())
                    .append(" if not exists = ")
                    .append(createIfNotExists)
                    .commit();

            clickhouseClient.createDatabase(declaration.getTableIdentity().getDatabaseName(), createIfNotExists);
        }

        debug()
                .append("Creating table ")
                .append(declaration.getTableIdentity().getTableName())
                .append(" if not exists = ")
                .append(createIfNotExists)
                .commit();

        clickhouseClient.createTable(declaration, ParseHelper.getMergeTreeEngine(declaration), createIfNotExists);
    }

    public final void send(T message) {
        if (inClosing.get())
            throw new IllegalArgumentException(String.format("Table writer `%s` closing.", WRITER_ID.toString()));

        if (message == null)
            throw new IllegalArgumentException("Message cannot be null.");

        try {
            messages.put(message);
            flusher.autoResetEvent.set();
        } catch (InterruptedException ex) {
            error()
                    .append("message processing failed.")
                    .append(System.lineSeparator())
                    .append(ex)
                    .commit();
        }
    }

    @Override
    public void close() {
        if (inClosing.compareAndSet(false, true)) {
            debug()
                    .append("closing")
                    .commit();

            flusher.autoResetEvent.set();

            try {
                flusherThread.join();
            } catch (InterruptedException e) {
                LOG.info()
                        .append("Flushed thread was interrupted.");
            }
        } else {
            debug()
                    .append("ignoring close request: already closing.")
                    .commit();
        }
    }

    private static LogEntry debug() {
        return LOG.debug()
                .append("Writer ")
                .append(WRITER_ID)
                .append(": ");
    }

    private static LogEntry error() {
        return LOG.error()
                .append("Writer ")
                .append(WRITER_ID)
                .append(": ");
    }
}