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
package com.epam.deltix.timebase.connector.clickhouse.services;

import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.qsrv.hf.pub.md.ClassSet;
import com.epam.deltix.qsrv.hf.pub.md.NamedDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;
import com.epam.deltix.qsrv.hf.tickdb.comm.UnknownStreamException;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickDB;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.qsrv.hf.tickdb.pub.SelectionOptions;
import com.epam.deltix.timebase.connector.clickhouse.ClickhouseConnectorApplication;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ReplicationProperties;
import com.epam.deltix.timebase.connector.clickhouse.algos.QueryReplicator;
import com.epam.deltix.timebase.connector.clickhouse.algos.Replicator;
import com.epam.deltix.timebase.connector.clickhouse.algos.StreamReplicator;
import com.epam.deltix.timebase.connector.clickhouse.model.QueryRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.ReplicationRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import com.epam.deltix.util.lang.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.epam.deltix.timebase.connector.clickhouse.algos.QueryReplicator.QUERY_STATUS_MESSAGE_TYPE;

@Service
public class ReplicatorService {

    private static final Log LOG = LogFactory.getLog(ReplicatorService.class);

    private final DXTickDB tickDb;
    private final ClickhouseClient clickhouseClient;
    private final ClickhouseProperties clickhouseProperties;
    private final ReplicationProperties replicationProperties;
    private final Map<Replicator, Thread> replicators = new ConcurrentHashMap<>();
    private Timer timer;

    @Autowired
    public ReplicatorService(ClickhouseClient clickhouseClient, DXTickDB tickDb,
                             ClickhouseProperties clickhouseProperties, ReplicationProperties replicationProperties) {
        this.clickhouseClient = clickhouseClient;
        this.tickDb = tickDb;
        this.clickhouseProperties = clickhouseProperties;
        this.replicationProperties = replicationProperties;

        LOG.info().append("Starting Clickhouse Replicator Service ").append(ClickhouseConnectorApplication.VERSION).commit();
    }


    @PostConstruct
    private void init() {
        connectionCheck();
        if (replicationProperties.getPollingIntervalMs() > 0)
            startTimer();
        else
            startReplicators();
    }

    private void connectionCheck() {
        boolean notReady = true;
        boolean clickhouseReady = false;
        while (notReady) {
            if (!tickDb.isOpen()) {
                try {
                    LOG.info().append("Opening connection to TimeBase on ").append(tickDb.toString()).commit();
                    tickDb.open(true);
                } catch (Exception e) {
                    LOG.error().append("Can't open timebase ").append(tickDb.toString()).append(" ")
                            .append(e.getMessage()).commit();
                }
            }
            if (!clickhouseReady) {
                try {
                    LOG.info().append("Connection to clickhouse ").append(clickhouseProperties.getUrl()).commit();
                    Connection connection = clickhouseClient.getConnection();
                    clickhouseReady = connection.isValid(clickhouseProperties.getTimeout());
                } catch (Exception e) {
                    LOG.error().append("Can't connect to clickhouse ").append(clickhouseProperties.getUrl()).append(" ")
                            .append(e.getMessage()).commit();
                }
            }

            notReady = !tickDb.isOpen() || !clickhouseReady;
            if (notReady) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    @PreDestroy
    public void destroy() {

        LOG.info().append("Dispose ReplicatorService.").commit();
        if (timer != null) {
            timer.cancel();
        }

        // first stop all threads
        for (Map.Entry<Replicator, Thread> entry : replicators.entrySet())
            entry.getKey().stop();

        // then wait for completion
        for (Map.Entry<Replicator, Thread> entry : replicators.entrySet()) {
            Thread replicatorThread = entry.getValue();

            try {
                LOG.info()
                        .append("Joining thread '")
                        .append(replicatorThread.getName())
                        .append("'")
                        .commit();

                replicatorThread.join();
            } catch (InterruptedException e) {
                // log and continue
                LOG.info()
                        .append("Thread '")
                        .append(replicatorThread.getName())
                        .append("' interrupted.")
                        .commit();
            }
        }

        LOG.info().append("ReplicatorService disposed.").commit();
    }

    private void startTimer() {
        timer = new java.util.Timer("long polling");
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                startReplicators();
            }
        }, 0, replicationProperties.getPollingIntervalMs());
    }

    public void startReplicators() {
        ArrayList<ReplicationRequest> requests = new ArrayList<>(replicationProperties.getStreams());
        requests.addAll(replicationProperties.getQueries());
        for (ReplicationRequest request : requests) {
            if (request.getIncludePartitionColumn() == null) {
                request.setIncludePartitionColumn(replicationProperties.isIncludePartitionColumn());
            }
            if (request.getColumnNamingScheme() == null) {
                request.setColumnNamingScheme(replicationProperties.getColumnNamingScheme());
            }
        }

        List<QueryRequest> queryRequests = replicationProperties.getQueries();
        List<StreamRequest> streamRequests = replicationProperties.getStreams();
        validateRequests(queryRequests, streamRequests);

        Set<String> runningReplicationKeys = replicators.keySet().stream().map(Replicator::getKey).collect(Collectors.toSet());
        streamRequests.stream()
                .filter(sr -> !runningReplicationKeys.contains(sr.getKey()))
                .forEach(streamRequest -> {
                    Replicator replicator = new StreamReplicator(streamRequest, tickDb, clickhouseClient, clickhouseProperties,
                    replicationProperties.getFlushMessageCount(),
                    replicationProperties.getFlushTimeoutMs(), this::onReplicatorStopped);
                    Thread replicatorThread = new Thread(replicator, String.format("Stream replicator '%s'", replicator.getKey()));

                    replicators.put(replicator, replicatorThread);
                    replicatorThread.start();
                });

        queryRequests.stream()
                .filter(qr -> !runningReplicationKeys.contains(qr.getKey()))
                .forEach(queryRequest -> {
            Replicator replicator = new QueryReplicator(queryRequest, tickDb, clickhouseClient, clickhouseProperties,
                    replicationProperties.getFlushMessageCount(),
                    replicationProperties.getFlushTimeoutMs(), this::onReplicatorStopped);
            Thread replicatorThread = new Thread(replicator, String.format("Query replicator '%s'", replicator.getKey()));

            replicators.put(replicator, replicatorThread);
            replicatorThread.start();
        });
    }

    private void validateRequests(List<QueryRequest> queries, List<StreamRequest> streamRequests) {
        List<String> allTargetTables = new ArrayList<>();
        for (StreamRequest streamRequest : streamRequests) {
            RecordClassSet recordClassSet = tryGetSchema(streamRequest);
            if (streamRequest.getTypeTableMapping() != null && !streamRequest.getTypeTableMapping().isEmpty()) {
                allTargetTables.addAll(streamRequest.getTypeTableMapping().values());
            } else if (streamRequest.isSplitByTypes()) {
                allTargetTables.addAll(getSplitTables(recordClassSet));
            } else {
                allTargetTables.add(streamRequest.getTable());
            }
        }
        for (QueryRequest queryRequest : queries) {
            RecordClassSet recordClassSet = tryGetSchema(queryRequest);
            if (queryRequest.getTypeTableMapping() != null && !queryRequest.getTypeTableMapping().isEmpty()) {
                allTargetTables.addAll(queryRequest.getTypeTableMapping().values());
            } else if (queryRequest.isSplitByTypes()) {
                allTargetTables.addAll(getSplitTables(recordClassSet));
            } else {
                allTargetTables.add(queryRequest.getTable());
            }
        }
        validateTargetTables(allTargetTables);
    }

    private RecordClassSet tryGetSchema(QueryRequest queryRequest) {
        try {
            ClassSet<?> classSet = tickDb.describeQuery(queryRequest.getQuery(), new SelectionOptions(true, false));
            RecordClassDescriptor[] contentClasses = Arrays.stream(classSet.getContentClasses())
                    .filter(rcd -> rcd instanceof RecordClassDescriptor && !rcd.getName().equals(QUERY_STATUS_MESSAGE_TYPE))
                    .map(rcd -> (RecordClassDescriptor)rcd).toArray(RecordClassDescriptor[]::new);
            return new RecordClassSet(contentClasses);
        } catch (Exception e) {
            LOG.error().append("Incorrect query for replication: ").append(queryRequest.getKey()).append(" Reason: ").append(e).commit();
            throw new IllegalArgumentException("Incorrect query for replication: " + queryRequest.getKey(), e);
        }
    }

    private RecordClassSet tryGetSchema(StreamRequest streamRequest) {
        DXTickStream stream = tickDb.getStream(streamRequest.getStream());
        if (stream == null) {
            LOG.error().append("Not found stream for replication: ").append(streamRequest.getKey()).commit();
            throw new UnknownStreamException(streamRequest.getStream());
        }
        return new RecordClassSet(stream.isFixedType() ?
                new RecordClassDescriptor[]{stream.getFixedType()} :
                stream.getPolymorphicDescriptors());
    }
    private static void validateTargetTables(List<String> allTargetTables) {
        Set<String> uniqueTables = new HashSet<>();
        for (String table : allTargetTables) {
            if (uniqueTables.contains(table)) {
                LOG.error().append("Incorrect settings. Multiple replications trying to write to the same table: ").append(table).commit();
                throw new IllegalArgumentException("Incorrect settings. Multiple replications trying to write to the same table: " + table);
            } else {
                uniqueTables.add(table);
            }
        }
    }

    private Collection<String> getSplitTables(RecordClassSet recordClassSet) {
        return Arrays.stream(recordClassSet.getTopTypes())
                .map(NamedDescriptor::getName)
                .map(Util::getSimpleName)
                .collect(Collectors.toList());
    }

    private Thread onReplicatorStopped(Replicator streamReplicator) {
        return replicators.remove(streamReplicator);
    }
}