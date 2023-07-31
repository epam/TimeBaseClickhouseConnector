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

import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.timebase.connector.clickhouse.timebase.array.AllArrayTypes;
import com.epam.deltix.timebase.connector.clickhouse.timebase.fieldNaming.*;
import com.epam.deltix.timebase.connector.clickhouse.util.Util;
import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassDescriptor;
import com.epam.deltix.qsrv.hf.pub.md.RecordClassSet;
import com.epam.deltix.qsrv.hf.tickdb.pub.DXTickStream;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickLoader;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;
import com.epam.deltix.util.collections.DuplicateKeyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

import static com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor.*;
import static com.epam.deltix.util.lang.Util.getSimpleName;
import static org.junit.jupiter.api.Assertions.*;

public class ColumnNameTests extends BaseStreamReplicatorTests {

    @Timeout(10)
    @ParameterizedTest
    @MethodSource("columnNamePatternOnExpectedColumnNames")
    void columnNamePatternsTest(ColumnNamingScheme pattern, Set<String> expectedValues) {
        Set<String> commonColumnsName = new HashSet<>(){{add(PARTITION_COLUMN_NAME);
            add(TIMESTAMP_COLUMN_NAME); add(INSTRUMENT_COLUMN_NAME); add(TYPE_COLUMN_NAME);}};
        commonColumnsName.addAll(expectedValues);
        expectedValues = commonColumnsName;

        AllArrayTypes message = new AllArrayTypes();
        initSystemRequiredFields(message);

        TableDeclaration tableDeclaration = loadAndReplicateData(new InstrumentMessage[]{message}, pattern, AllArrayTypes.class);

        Map<String, Object> values = selectAllValues(tableDeclaration).get(0);

        assertEquals(expectedValues.size(), values.size());
        assertTrue(expectedValues.containsAll(values.keySet()));
    }

    public Stream<Arguments> columnNamePatternOnExpectedColumnNames() {
        return Stream.of(
                Arguments.of(ColumnNamingScheme.TYPE_AND_NAME,
                        Set.of("AllArrayTypes_doubleArray",
                        "AllArrayTypes_floatArray",
                        "AllArrayTypes_enumArray",
                        "AllArrayTypes_timeOfDayList",
                        "AllArrayTypes_longArray",
                        "AllArrayTypes_shortArray",
                        "AllArrayTypes_byteArray",
                        "AllArrayTypes_timestampList")),
                Arguments.of(ColumnNamingScheme.NAME,
                        Set.of( "doubleArray",
                                "floatArray",
                                "enumArray",
                                "timeOfDayList",
                                "longArray",
                                "shortArray",
                                "byteArray",
                                "timestampList")),
                Arguments.of(ColumnNamingScheme.NAME_AND_DATATYPE,
                        Set.of( "doubleArray_Array_Nullable_Float64",
                                "floatArray_Array_Nullable_Float32",
                                "enumArray_Array_Nullable_String",
                                "timeOfDayList_Array_Nullable_Int32",
                                "longArray_Array_Nullable_Int64",
                                "shortArray_Array_Nullable_Int16",
                                "byteArray_Array_Nullable_Int8",
                                "timestampList_Array_Nullable_DateTime64_3_"))
        );
    }

    @Timeout(10)
    @Test
    void createSchemaWithDuplicateColumnNamesByPattern_splitOnDifTables_ok() {
        FirstSubFieldObject firstSubField = new FirstSubFieldObject();
        SecondSubFieldObject secondSubField = new SecondSubFieldObject();
        initSystemRequiredFields(firstSubField);
        initSystemRequiredFields(secondSubField);
        InstrumentMessage[] messages = {firstSubField, secondSubField};

        DXTickStream stream = loadData(messages, FirstSubFieldObject.class, SecondSubFieldObject.class);
        RecordClassSet tbSchema = new RecordClassSet(stream.getPolymorphicDescriptors());
        HashMap<String, String> mapping = new HashMap<>();
        RecordClassDescriptor[] topTypes = tbSchema.getTopTypes();
        for (int i = 0; i < topTypes.length; i++) {
            mapping.put(topTypes[i].getName(), getSimpleName(topTypes[i].getName()));
        }
        SchemaOptions schemaOptions = new SchemaOptions(tbSchema, mapping, WriteMode.REWRITE, ColumnNamingScheme.NAME, true);
        Map<String, TableDeclaration> tableDeclaration = Util.getTableDeclaration(schemaOptions);
        for (TableDeclaration value : tableDeclaration.values()) {
            assertEquals(6, value.getColumns().size());
        }
    }

    @Test
    void createSchemaWithDuplicateColumnNamesByPattern_singleTable_failed() {
        FirstSubFieldObject firstSubField = new FirstSubFieldObject();
        SecondSubFieldObject secondSubField = new SecondSubFieldObject();
        initSystemRequiredFields(firstSubField);
        initSystemRequiredFields(secondSubField);
        InstrumentMessage[] messages = {firstSubField, secondSubField};

        DXTickStream stream = loadData(messages, FirstSubFieldObject.class, SecondSubFieldObject.class);
        assertThrows(DuplicateKeyException.class, () -> {
            Util.getTableDeclaration(stream, ColumnNamingScheme.NAME);
        });
    }

    @Test
    void createSchemaWithDuplicateColumnNamesByPattern_singleTable_mergeColumns() {
        FirstMessage message = new FirstMessage();
        SecondMessage secondMessage = new SecondMessage();
        initSystemRequiredFields(message);
        initSystemRequiredFields(secondMessage);
        InstrumentMessage[] messages = {message, secondMessage};

        DXTickStream stream = loadData(messages, FirstMessage.class, SecondMessage.class);
        TableDeclaration tableDeclaration = Util.getTableDeclaration(stream, ColumnNamingScheme.NAME);
        assertEquals(6, tableDeclaration.getColumns().size());
    }

    @Test
    void createSchemaWitDuplicateNestedColumns_byNamePattern_failed() {
        FieldNameMessage message = new FieldNameMessage();
        initSystemRequiredFields(message);

        DXTickStream stream = loadData(message, FieldNameMessage.class, FirstSubFieldObject.class, SecondSubFieldObject.class);
        assertThrows(DuplicateKeyException.class, () -> {
            Util.getTableDeclaration(stream, ColumnNamingScheme.NAME);
        });
    }

    @Test
    void createSchemaWitDuplicateNestedColumns_byNameChDtPattern_mergeIdenticalColumns() {
        FieldNameMessage message = new FieldNameMessage();
        initSystemRequiredFields(message);

        DXTickStream stream = loadData(message, FieldNameMessage.class, FirstSubFieldObject.class, SecondSubFieldObject.class);
        TableDeclaration tableDeclaration = Util.getTableDeclaration(stream, ColumnNamingScheme.NAME_AND_DATATYPE);
        assertColumnsCount(tableDeclaration, 13);
    }

    @Test
    void createSchemaWitDuplicateNestedColumns_byTypeNamePattern_noMergeIdenticalColumns() {
        FieldNameMessage message = new FieldNameMessage();
        initSystemRequiredFields(message);

        DXTickStream stream = loadData(message, FieldNameMessage.class, FirstSubFieldObject.class, SecondSubFieldObject.class);
        TableDeclaration tableDeclaration = Util.getTableDeclaration(stream, ColumnNamingScheme.TYPE_AND_NAME);
        assertColumnsCount(tableDeclaration, 15);
    }

    private TableDeclaration loadAndReplicateData(final InstrumentMessage[] messages, ColumnNamingScheme pattern, java.lang.Class... types) {
        DXTickStream stream = loadData(messages, types);
        TableDeclaration tableDeclaration = getTableDeclaration(stream, pattern);
        startStreamReplication(stream, tableDeclaration.getTableIdentity(), Util.getStreamRequest(stream, pattern), messages.length);

        return tableDeclaration;
    }

    private DXTickStream loadData(final InstrumentMessage[] messages, java.lang.Class... types) {
        String streamKey = String.format("%s_%s", messages[0].getClass().getName(), Instant.now());

        Class<?>[] streamTypes = new Class[messages.length];
        for (int i = 0; i < messages.length; i++) {
            streamTypes[i] = messages[i].getClass();
        }

        DXTickStream stream;
        TickLoader loader = null;
        try {

            stream = createStream(tickDB, streamKey, streamTypes);
            loader = createLoader(stream, types);

            for (InstrumentMessage message : messages) {
                loader.send(message);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (loader != null)
                loader.close();
        }

        return stream;
    }
}