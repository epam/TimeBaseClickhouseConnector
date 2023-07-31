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
package com.epam.deltix.timebase.connector.clickhouse;

import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.timebase.connector.clickhouse.timebase.fieldNaming.FieldNameMessage;
import com.epam.deltix.timebase.connector.clickhouse.timebase.fieldNaming.FirstSubFieldObject;
import com.epam.deltix.timebase.connector.clickhouse.timebase.fieldNaming.SecondSubFieldObject;
import com.epam.deltix.timebase.connector.clickhouse.util.Util;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.timebase.connector.clickhouse.model.StreamRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.SQLException;
import java.util.*;

import static com.epam.deltix.timebase.connector.clickhouse.algos.Replicator.ALL_TYPES;
import static com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor.normalizeStreamNameToClickhouseNotation;
import static org.junit.jupiter.api.Assertions.*;

public class CustomTableSchemaTests extends BaseStreamReplicatorTests {

    private DXTickStream stream;
    private String tableName;

    @BeforeEach
    void initStream() {
        FieldNameMessage message = new FieldNameMessage();
        initSystemRequiredFields(message);
        stream = loadData(message, FieldNameMessage.class, FirstSubFieldObject.class, SecondSubFieldObject.class);
        tableName = CLICKHOUSE_DATABASE_NAME + "." + normalizeStreamNameToClickhouseNotation(stream.getKey());
    }

    @Timeout(10)
    @Test
    void replicationInExistingTableWithDifferentSchema() throws SQLException {
        String sqlExpression = "CREATE TABLE " + tableName + "\n" +
                "(\n" +
                "    `partition` Date,\n" +
                "    `timestamp` DateTime64(9),\n" +
                "    `instrument` String,\n" +
                "    `type` String,\n" +
                "    `FieldNameMessage_objectArray.type` Array(Nullable(String)),\n" +
                "    `FieldNameMessage_objectArray.FirstSubFieldObject_int32Field` Array(Nullable(Int32)),\n" +
                "    `FieldNameMessage_objectArray.FirstSubFieldObject_simpleField` Array(Nullable(String)),\n" +
                "    `FieldNameMessage_objectArray.SecondSubFieldObject_int32Field` Array(Nullable(Int32)),\n" +
                "    `FieldNameMessage_objectArray.SecondSubFieldObject_simpleField` Array(Nullable(Enum16('FIRST' = 0, 'SECOND' = 1, 'DEFAULT' = 2))),\n" +
//skipped field "    `FieldNameMessage_simpleField` Nullable(String),\n" +
                "    `FieldNameMessage_simpleObject_type` Nullable(String),\n" +
//skipped field "    `FieldNameMessage_simpleObject_FirstSubFieldObject_int32Field` Nullable(Int32),\n" +
                "    `FieldNameMessage_simpleObject_FirstSubFieldObject_simpleField` Nullable(String),\n" +
                "    `FieldNameMessage_simpleObject_SecondSubFieldObject_int32Field` Nullable(Int32),\n" +
                "    `FieldNameMessage_simpleObject_SecondSubFieldObject_simpleField` Nullable(Enum16('FIRST' = 0, 'SECOND' = 1, 'DEFAULT' = 2))\n" +
                ")\n" +
                "ENGINE = MergeTree\n" +
                "PARTITION BY partition\n" +
                "ORDER BY (timestamp, instrument)\n" +
                "SETTINGS index_granularity = 8192";
        clickhouseClient.executeExpression(sqlExpression);
        TableDeclaration clickhouseTable = prepareClickhouseTable(stream).get(ALL_TYPES);
        TableDeclaration tableDeclaration = Util.getTableDeclaration(stream);

        assertColumnsCount(clickhouseTable, 13);
        assertColumnsCount(tableDeclaration, 15);

        startStreamReplication(stream, clickhouseTable.getTableIdentity());

        Map<String, Object> values = selectAllValues(clickhouseTable).get(0);

        assertEquals(13, values.size());
        assertFalse(values.containsKey("FieldNameMessage_simpleObject_FirstSubFieldObject_int32Field"));
        assertFalse(values.containsKey("FieldNameMessage_simpleField"));
        assertTrue(values.containsKey("FieldNameMessage_objectArray.SecondSubFieldObject_simpleField"));
        assertTrue(values.containsKey("FieldNameMessage_simpleObject_SecondSubFieldObject_int32Field"));

        clickhouseClient.dropTable(tableDeclaration.getTableIdentity());
    }

    @Timeout(10)
    @Test
    void replicationInExistingTableWithoutPartitionColumn() throws SQLException {
        String sqlExpression = "CREATE TABLE " + tableName + "\n" +
                "(\n" +
//                "    `partition` Date,\n" +
                "    `timestamp` DateTime64(9),\n" +
                "    `instrument` String,\n" +
                "    `type` String,\n" +
                "    `FieldNameMessage_objectArray.type` Array(Nullable(String)),\n" +
                "    `FieldNameMessage_objectArray.FirstSubFieldObject_int32Field` Array(Nullable(Int32)),\n" +
                "    `FieldNameMessage_objectArray.FirstSubFieldObject_simpleField` Array(Nullable(String)),\n" +
                "    `FieldNameMessage_objectArray.SecondSubFieldObject_int32Field` Array(Nullable(Int32)),\n" +
                "    `FieldNameMessage_objectArray.SecondSubFieldObject_simpleField` Array(Nullable(Enum16('FIRST' = 0, 'SECOND' = 1, 'DEFAULT' = 2))),\n" +
                "    `FieldNameMessage_simpleField` Nullable(String),\n" +
                "    `FieldNameMessage_simpleObject_type` Nullable(String),\n" +
                "    `FieldNameMessage_simpleObject_FirstSubFieldObject_int32Field` Nullable(Int32),\n" +
                "    `FieldNameMessage_simpleObject_FirstSubFieldObject_simpleField` Nullable(String),\n" +
                "    `FieldNameMessage_simpleObject_SecondSubFieldObject_int32Field` Nullable(Int32),\n" +
                "    `FieldNameMessage_simpleObject_SecondSubFieldObject_simpleField` Nullable(Enum16('FIRST' = 0, 'SECOND' = 1, 'DEFAULT' = 2))\n" +
                ")\n" +
                "ENGINE = MergeTree\n" +
//                "PARTITION BY partition\n" +
                "ORDER BY (timestamp, instrument)\n" +
                "SETTINGS index_granularity = 8192";
        clickhouseClient.executeExpression(sqlExpression);

        TableDeclaration clickhouseTable = prepareClickhouseTable(stream).get(ALL_TYPES);
        TableDeclaration tableDeclaration = Util.getTableDeclaration(stream);
        assertFalse(clickhouseTable.getColumns().stream().map(ColumnDeclaration::getDbColumnName).anyMatch(SchemaProcessor.PARTITION_COLUMN_NAME::equals));
        assertTrue(tableDeclaration.getColumns().stream().map(ColumnDeclaration::getDbColumnName).anyMatch(SchemaProcessor.PARTITION_COLUMN_NAME::equals));

        StreamRequest streamRequest = new StreamRequest();
        streamRequest.setKey(stream.getKey());
        streamRequest.setStream(stream.getKey());
        streamRequest.setColumnNamingScheme(ColumnNamingScheme.TYPE_AND_NAME);
        streamRequest.setIncludePartitionColumn(false);
        startStreamReplication(stream, tableDeclaration.getTableIdentity(), 1, 1, 1, streamRequest);

        Map<String, Object> values = selectAllValues(clickhouseTable).get(0);
        assertFalse(values.containsKey(SchemaProcessor.PARTITION_COLUMN_NAME));

        clickhouseClient.dropTable(tableDeclaration.getTableIdentity());
    }

    @Timeout(10)
    @Test
    void createTableWithoutPartitionColumn() {
        TableDeclaration tableDeclaration = null;

        try {
            RecordClassSet classSet = new RecordClassSet(stream.isFixedType() ?
                    new RecordClassDescriptor[]{stream.getFixedType()} :
                    stream.getPolymorphicDescriptors());
            Map<String, String> mapping = new HashMap<>() {{
                put(ALL_TYPES, stream.getKey());
            }};

            tableDeclaration = Util.getTableDeclaration(
                    new SchemaOptions(classSet, mapping, WriteMode.APPEND, ColumnNamingScheme.TYPE_AND_NAME, false)).get(ALL_TYPES);

            assertFalse(tableDeclaration.getColumns().stream().map(ColumnDeclaration::getDbColumnName).anyMatch(SchemaProcessor.PARTITION_COLUMN_NAME::equals));
            assertFalse(clickhouseClient.existsTable(tableDeclaration.getTableIdentity()));
            StreamRequest streamRequest = new StreamRequest();
            streamRequest.setKey(stream.getKey());
            streamRequest.setStream(stream.getKey());
            streamRequest.setColumnNamingScheme(ColumnNamingScheme.TYPE_AND_NAME);
            streamRequest.setIncludePartitionColumn(false);
            startStreamReplication(stream, tableDeclaration.getTableIdentity(), 1, 1, 1, streamRequest);
            assertTrue(clickhouseClient.existsTable(tableDeclaration.getTableIdentity()));

            TableDeclaration actualTable = clickhouseClient.describeTable(tableDeclaration.getTableIdentity());
            assertFalse(actualTable.getColumns().stream().map(ColumnDeclaration::getDbColumnName).anyMatch(SchemaProcessor.PARTITION_COLUMN_NAME::equals));
            assertEquals(10, actualTable.getColumns().size());
            Map<String, Object> values = selectAllValues(tableDeclaration).get(0);
            assertFalse(values.containsKey(SchemaProcessor.PARTITION_COLUMN_NAME));
        } finally {
            clickhouseClient.dropTable(tableDeclaration.getTableIdentity());
        }

    }

    private Map<String, TableDeclaration> prepareClickhouseTable(DXTickStream stream) {
        SchemaProcessor schemaProcessor = new SchemaProcessor(Util.getSchemaOptions(stream, ColumnNamingScheme.TYPE_AND_NAME), clickhouseClient, clickhouseProperties);
        try {
            return schemaProcessor.prepareClickhouseTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}