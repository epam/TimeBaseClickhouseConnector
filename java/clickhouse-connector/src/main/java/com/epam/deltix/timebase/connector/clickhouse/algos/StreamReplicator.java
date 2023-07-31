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
package com.epam.deltix.timebase.connector.clickhouse.algos;

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.codec.CodecFactory;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.NamedDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;
import com.epam.deltix.qsrv.hf.tickdb.pub.*;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;
import com.epam.deltix.util.concurrent.UnavailableResourceException;
import com.epam.deltix.util.lang.Util;
import com.epam.deltix.util.memory.MemoryDataInput;
import com.epam.deltix.util.time.TimeKeeper;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class StreamReplicator extends Replicator {


    private final StreamRequest request;
    private volatile boolean cancel = false;
    private long lastFlushTimestamp = 0;
    private UnboundTableWriter tableWriter = null;
    private final Object unblockingCursorLock = new Object();


    public StreamReplicator(StreamRequest streamRequest,
                            DXTickDB tickDb,
                            ClickhouseClient clickhouseClient,
                            ClickhouseProperties clickhouseProperties,
                            int flushMessageCount,
                            long flushTimeoutMs,
                            Consumer<Replicator> onStopped) {
        super(tickDb, clickhouseClient, clickhouseProperties, onStopped, flushMessageCount, flushTimeoutMs);
        this.request = streamRequest;
    }

    @Override
    public void run() {
        long count = 0;
        int reportThreshold = Math.min(flushMessageCount * 10, 1_000_000);
        try {
            DXTickStream stream = tickDb.getStream(request.getStream());

            SchemaOptions schemaOptions = getSchemaOptions(stream);
            SchemaProcessor schemaProcessor = new SchemaProcessor(schemaOptions, clickhouseClient, clickhouseProperties);

            LOG.info()
                    .append("Replication ")
                    .append(request.getKey())
                    .append(": prepare target table schema.")
                    .commit();
            Map<String, TableDeclaration> clickhouseTables = schemaProcessor.prepareClickhouseTable();

            long from = Long.MIN_VALUE;
            if (WriteMode.APPEND == request.getWriteMode()) {
                from = findLastTimestamp(clickhouseTables.values());
                truncateData(clickhouseTables.values(), from);
            }

            SelectionOptions selectionOptions = new SelectionOptions(true, true);

            MemoryDataInput in = new MemoryDataInput();
            RecordClassDescriptor[] descriptors = schemaOptions.getTbSchema().getContentClasses();
            List<UnboundDecoder> decoders = Arrays.stream(descriptors).map(CodecFactory.COMPILED::createFixedUnboundDecoder).collect(Collectors.toList());

            tableWriter = new UnboundTableWriter(request.getKey(), request.getColumnNamingScheme(), clickhouseClient,
                    clickhouseTables,  schemaProcessor.getColumnDeclarations(), decoders, in/*, 10_000, 5_000*/);
            if (!request.getIncludePartitionColumn()) {
                tableWriter.removeFixedColumn(SchemaProcessor.PARTITION_COLUMN_NAME);
            }
            try (TickCursor cursor = stream.select(from, selectionOptions)) {
                // making live cursor non-blocking
                cursor.setAvailabilityListener(this::notifyDataAvailable);

                do {
                    if (cancel)
                        break;

                    if (tableWriter.getBatchMsgCount() > 0) { // we have messages in queue
                        if (tableWriter.getBatchMsgCount() >= flushMessageCount || // batch size reached
                                TimeKeeper.currentTime >= lastFlushTimestamp + flushTimeoutMs) // flush interval reached
                            flush();
                    }

                    synchronized (unblockingCursorLock) {
                        try {
                            if (cursor.next())
                                tableWriter.send((RawMessage) cursor.getMessage(), cursor);
                            else
                                break;
                            count++;
                        } catch (UnavailableResourceException e) {
                            try {
                                long timeout = flushTimeoutMs - (TimeKeeper.currentTime - lastFlushTimestamp);
                                if (timeout > 0) {
                                    unblockingCursorLock.wait(timeout);
                                } else {
                                    lastFlushTimestamp = TimeKeeper.currentTime;
                                }
                            } catch (InterruptedException ie) {
                                // continue
                            }
                        }

                        if (count % reportThreshold == 0 && count > 0)
                            LOG.info().append("Replication ").append(request.getKey())
                                    .append(": write ").append(count).append(" messages.").commit();
                    }
                } while (true);
            }
            LOG.info()
                    .append("Replication ")
                    .append(request.getKey())
                    .append(": read process finished. Stopping.")
                    .commit();

            //flush(); // slice might be incomplete
        } catch (Throwable e) {
            LOG.error()
                    .append("Replication ")
                    .append(request.getKey())
                    .append(": unhandled exception during replication process. Stopping.")
                    .append(e)
                    .commit();
        } finally {
            if (tableWriter != null)
                tableWriter.close();
            onStopped.accept(this);
            LOG.info()
                    .append("Replication ")
                    .append(request.getKey())
                    .append(": stopped. Replicated ")
                    .append(count).append(" messages.")
                    .commit();
        }
    }

    @Override
    public void stop() {
        cancel = true;
    }

    private void notifyDataAvailable() {
        synchronized (unblockingCursorLock) {
            unblockingCursorLock.notify();
        }
    }

    private void flush() throws SQLException {
        tableWriter.flush();
        lastFlushTimestamp = TimeKeeper.currentTime;
    }

    private SchemaOptions getSchemaOptions(DXTickStream stream) {
        RecordClassSet classSet = new RecordClassSet(stream.isFixedType() ?
                new RecordClassDescriptor[]{stream.getFixedType()} :
                stream.getPolymorphicDescriptors());

        Map<String, String> mapping;
        if (!request.getTypeTableMapping().isEmpty()) {
            mapping = request.getTypeTableMapping();
        } else if (request.isSplitByTypes()) {
            mapping = Arrays.stream(classSet.getTopTypes())
                    .map(NamedDescriptor::getName)
                    .collect(Collectors.toMap(name -> name, Util::getSimpleName));
        } else {
            mapping = new HashMap<>() {{
                put(ALL_TYPES, request.getTable());
            }};
        }

        return new SchemaOptions(classSet, mapping, request.getWriteMode(), request.getColumnNamingScheme(), request.getIncludePartitionColumn());
    }

    @Override
    public String getKey() {
        return request.getKey();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamReplicator that = (StreamReplicator) o;

        return request.equals(that.request);
    }

    @Override
    public int hashCode() {
        return request.hashCode();
    }
}