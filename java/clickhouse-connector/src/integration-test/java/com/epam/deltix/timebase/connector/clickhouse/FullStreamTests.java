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

import com.clickhouse.data.value.UnsignedByte;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.stream.MessageReader2;
import com.epam.deltix.qsrv.hf.tickdb.pub.*;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.QueryRequest;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;
import com.epam.deltix.util.time.Periodicity;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static com.epam.deltix.util.lang.Util.getSimpleName;
import static org.junit.jupiter.api.Assertions.*;

public class FullStreamTests extends BaseStreamReplicatorTests {

    public static final String ALL_TYPES_MESSAGES_QSMSG_GZ = "allTypesMessages.qsmsg.gz";

    @Test
    @Timeout(60)
    void streamReplicateAllTypesMessageTest() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader(ALL_TYPES_MESSAGES_QSMSG_GZ);
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateData(reader, 54, ColumnNamingScheme.TYPE_AND_NAME);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            compareAllTypesValues(expected, actual, message.type, ColumnNamingScheme.TYPE_AND_NAME);
        }
    }

    @Test
    @Timeout(60)
    void queryReplicateAllTypesMessageTest() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader(ALL_TYPES_MESSAGES_QSMSG_GZ);
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateQueryData(reader, 54, ColumnNamingScheme.TYPE_AND_NAME);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            compareAllTypesValues(expected, actual, message.type, ColumnNamingScheme.TYPE_AND_NAME);
        }
    }

    @Test
    @Timeout(60)
    void queryReplicateAllTypesMessage_NAME_AND_DATATYPE_ColumnNamingScheme() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader(ALL_TYPES_MESSAGES_QSMSG_GZ);
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateQueryData(reader, 54, ColumnNamingScheme.NAME_AND_DATATYPE);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            compareAllTypesValues(expected, actual, message.type, ColumnNamingScheme.NAME_AND_DATATYPE);
        }
    }
    @Test
    @Timeout(60)
    void queryReplicatePackageHeaderMessageTest() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader("PackageHeader.qsmsg.gz");
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateQueryData(reader, 2031, ColumnNamingScheme.TYPE_AND_NAME);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            comparePackageHeaderValues(expected, actual, message.type);
        }
    }
    
    void queryReplicateSecurities() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader("smd.qsmsg.gz");
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateQueryData(reader, 305, ColumnNamingScheme.TYPE_AND_NAME);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            comparePackageHeaderValues(expected, actual, message.type);
        }
    }

    @Test
    @Timeout(60)
    void streamReplicatePackageHeaderMessageTest() throws URISyntaxException, IOException, ParseException {
        RawMessageHelper helper = new RawMessageHelper();
        MessageReader2 reader = createReader("PackageHeader.qsmsg.gz");
        Pair<DXTickStream, TableDeclaration> pair = loadAndReplicateData(reader, 2031, ColumnNamingScheme.TYPE_AND_NAME);

        List<Map<String, Object>> maps = selectAllValues(pair.getRight());
        TickCursor select = pair.getLeft().select(0, new SelectionOptions(true, false));
        int i = 0;
        while (select.next()) {
            RawMessage message = (RawMessage) select.getMessage();
            Map<String, Object> actual = maps.get(i++);
            commonFieldsCheck(message, actual);
            Map<String, Object> expected = helper.getValues(message);
            comparePackageHeaderValues(expected, actual, message.type);
        }
    }


    private void commonFieldsCheck(RawMessage message, Map<String, Object> actual) throws ParseException {
        assertEquals(message.type.getName(), actual.get("type"));
        assertEquals(message.getTimeStampMs(), convertTimestamp(actual.get("timestamp")));
        assertEquals(message.getSymbol(), actual.get("instrument"));

    }

    private void comparePackageHeaderValues(Map<String, Object> expected, Map<String, Object> actual, RecordClassDescriptor type) throws ParseException {
        String typeName = getSimpleName(type.getName());
        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            String fieldName = getFieldName(typeName, entry.getKey(), ColumnNamingScheme.TYPE_AND_NAME);
            if (entry.getKey().equals("entries")) {
                Object[] values = (Object[]) entry.getValue();
                for (int i = 0; i < values.length; i++) {
                    Map<String, Object> value = (Map<String, Object>) values[i];
                    String innerTypeName = getSimpleName(((RecordClassDescriptor) value.get("objectClassName")).getName());
                    for (Map.Entry<String, Object> innerEntry : value.entrySet()) {
                        if (innerEntry.getKey().equals(("objectClassName"))) continue;
                        String innerFieldName = fieldName + "." + innerTypeName + "_" + innerEntry.getKey();
                        Object[] nestedArr = (Object[]) actual.get(innerFieldName);
                        assertEquals(innerEntry.getValue(), convertValue(nestedArr[i]));
                    }
                }
            } else {
                if (entry.getKey().equals("originalTimestamp") || entry.getKey().equals("receivedTime")) {
                    assertEquals(entry.getValue(), convertTimestamp(actual.get(fieldName)));
                } else {
                    assertEquals(entry.getValue(), actual.get(fieldName));
                }
            }
        }
    }

    private void compareAllTypesValues(Map<String, Object> expected, Map<String, Object> actual, RecordClassDescriptor type, ColumnNamingScheme scheme) throws ParseException {
        String typeName = getSimpleName(type.getName());
        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            if (entry.getKey().equals("objectsListOfNullable") || entry.getKey().equals("objectsList") || entry.getKey().equals("nullableObjectsListOfNullable")
                    || entry.getKey().equals("listOfLists") || entry.getKey().equals("nullableObjectsList")) {
                Object[] values = (Object[]) entry.getValue();
                if (values == null) {
                    continue;
                }
                String fieldName = getBaseName(typeName, entry.getKey(), scheme);
                for (int i = 0; i < values.length; i++) {
                    Map<String, Object> value = (Map<String, Object>) values[i];
                    if (value != null) {
                        String innerTypeName = getSimpleName(((RecordClassDescriptor) value.get("objectClassName")).getName());
                        for (Map.Entry<String, Object> innerEntry : value.entrySet()) {
                            if (innerEntry.getKey().equals(("objectClassName"))) continue;
                            String innerFieldName = fieldName + "." + getFieldName(innerTypeName, innerEntry.getKey(), scheme);
                            Object[] nestedArr = (Object[]) actual.get(innerFieldName);
                            if (innerEntry.getKey().contains("binary")) {
                                assertEquals(covertBinary(innerEntry.getValue()), nestedArr[i]);
                            } else if (innerEntry.getKey().contains("estedDecimalList")) {
                                assertArrayDoubleValues(innerEntry.getValue(), nestedArr[i]);
                            } else if (innerEntry.getKey().contains("decimal")) {
                                assertDoubleValues(innerEntry.getValue(), nestedArr[i]);
                            } else if (innerEntry.getKey().contains("nestedBooleanList")) {
                                assertArrayEquals(new Object[]{convertByteValues(innerEntry.getValue())}, new Object[]{convertByteValues(nestedArr[i])});
                            } else {
                                assertArrayEquals(new Object[]{innerEntry.getValue()}, new Object[]{convertValue(nestedArr[i])});
                            }
                        }
                    }
                }
            } else if (entry.getKey().equals("lists") || entry.getKey().equals("object")) {
                String fieldName = getBaseName(typeName, entry.getKey(), scheme);
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                String innerTypeName = getSimpleName(((RecordClassDescriptor) value.get("objectClassName")).getName());
                for (Map.Entry<String, Object> innerEntry : value.entrySet()) {
                    if (innerEntry.getKey().equals(("objectClassName"))) continue;
                    String classSeparator = scheme == ColumnNamingScheme.NAME_AND_DATATYPE ? "_N_" : "_";
                    String innerFieldName = fieldName + classSeparator + getFieldName(innerTypeName, innerEntry.getKey(), scheme);
                    Object nestedVal = actual.get(innerFieldName);
                    if (innerEntry.getKey().contains("binary")) {
                        assertEquals(covertBinary(innerEntry.getValue()), nestedVal);
                    } else if (innerEntry.getKey().contains("estedDecimalList")) {
                        assertArrayEquals(new Object[]{getBigDecimalArr(innerEntry.getValue())}, new Object[]{nestedVal});
                    } else if (innerEntry.getKey().contains("decimal")) {
                        assertDoubleValues(innerEntry.getValue(), nestedVal);
                    } else if (innerEntry.getKey().contains("imestamp")) {
                        assertArrayEquals(new Object[]{innerEntry.getValue()}, new Object[]{convertTimestampArray(nestedVal)});
                    } else {
                        assertArrayEquals(new Object[]{innerEntry.getValue()}, new Object[]{convertValue(nestedVal)});
                    }
                }
            } else {
                String fieldName = getFieldName(typeName, entry.getKey(), scheme);
                if (entry.getKey().startsWith("nullable") && (entry.getKey().endsWith("List") || entry.getKey().endsWith("ListOfNullable")) && entry.getValue() == null) {
                    assertEquals(0, ((Object[]) actual.get(fieldName)).length);
                } else if (entry.getKey().contains("ecimalList")) {
                    assertArrayDoubleValues(entry.getValue(), actual.get(fieldName));
                } else if (entry.getKey().contains("decimal")) {
                    assertDoubleValues(entry.getValue(), actual.get(fieldName));
                } else if (entry.getKey().contains("ooleanList")) {
                    assertArrayEquals(new Object[]{convertByteValues(entry.getValue())}, new Object[]{convertByteValues(actual.get(fieldName))});
                } else if (entry.getKey().contains("binary")) {
                    assertEquals(covertBinary(entry.getValue()), actual.get(fieldName));
                } else if (entry.getKey().contains("imestamp")) {
                    assertArrayEquals(new Object[]{entry.getValue()}, new Object[]{convertTimestampArray(actual.get(fieldName))});
                } else {
                    assertArrayEquals(new Object[]{entry.getValue()}, new Object[]{convertValue(actual.get(fieldName))});
                }
            }
        }
    }

    private String getBaseName(String typeName, String field, ColumnNamingScheme scheme) {
        if (ColumnNamingScheme.NAME_AND_DATATYPE == scheme) {
            return field;
        } else {
            return typeName + "_" + field;
        }
    }

    private static String getFieldName(String typeName, String field, ColumnNamingScheme scheme) {
        if (ColumnNamingScheme.NAME_AND_DATATYPE == scheme) {
            String tmp = field.toLowerCase();
            if (tmp.startsWith("nested")) tmp = tmp.substring(6);
            DataType dbDataType = bindDbDatatype(tmp, field.startsWith("nested"));
            return SchemaProcessor.encodeColumnName(SchemaProcessor.getColumnNameWithDataType(dbDataType, SchemaProcessor.convertTimebaseDataTypeToClickhouseDataType(dbDataType), field));
        }
        return typeName + "_" + field;
    }

    private static DataType bindDbDatatype(String field, boolean primaryNullable) {
        if (field.contains("list")){
            return new ArrayDataType(primaryNullable || field.startsWith("nullable"), bindDbDatatype(getSubField(field), primaryNullable));
        }
        boolean nullable = primaryNullable || field.contains("nullable");
        if (field.startsWith("asciitext") || field.startsWith("text")) {
            return new VarcharDataType("UTF8", nullable, true);
        }
        if (field.startsWith("textalphanumeric") || field.startsWith("alphanumeric")) {
            return new VarcharDataType("ALPHANUMERIC(10)", nullable, true);
        }
        if (field.startsWith("binary")) {
            return new BinaryDataType(nullable, 0);
        }
        if (field.startsWith("bool")) {
            return new BooleanDataType(nullable);
        }
        if (field.startsWith("byte")) {
            return new IntegerDataType(IntegerDataType.ENCODING_INT8, nullable);
        }
        if (field.startsWith("short")) {
            return new IntegerDataType(IntegerDataType.ENCODING_INT16, nullable);
        }
        if (field.startsWith("int")) {
            return new IntegerDataType(IntegerDataType.ENCODING_INT32, nullable);
        }
        if (field.startsWith("long")) {
            return new IntegerDataType(IntegerDataType.ENCODING_INT64, nullable);
        }
        if (field.startsWith("decimal")) {
            return new FloatDataType(FloatDataType.ENCODING_DECIMAL64, nullable);
        }
        if (field.startsWith("double")) {
            return new FloatDataType(FloatDataType.ENCODING_FIXED_DOUBLE, nullable);
        }
        if (field.startsWith("float")) {
            return new FloatDataType(FloatDataType.ENCODING_FIXED_FLOAT, nullable);
        }
        if (field.startsWith("enum")) {
            return new EnumDataType(nullable, new EnumClassDescriptor("deltix.qsrv.test.messages.TestEnum", "enum", "ZERO", "ONE", "TWO", "THREE", "FOUR", "FIVE"));
        }
        if (field.startsWith("timeofday")) {
            return new TimeOfDayDataType(nullable);
        }
        if (field.startsWith("timestamp")) {
            return new DateTimeDataType(nullable);
        }
        return null;
    }

    private static String getSubField(String field) {
        if (field.startsWith("nullable")){
            field = field.substring(8);
        }
        return field.replace("list", "");
    }

    private void assertArrayDoubleValues(Object exp, Object act) {
        BigDecimal[] expDecimalArr = (BigDecimal[]) getBigDecimalArr(exp);
        BigDecimal[] actDecimalArr = (BigDecimal[]) act;
        for (int i = 0; i < expDecimalArr.length; i++) {
            BigDecimal exVal = expDecimalArr[i];
            BigDecimal actVal = actDecimalArr[i];
            if (exVal != actVal)
                assertDoubleValues(exVal.doubleValue(), actVal.doubleValue());
        }
    }

    private void assertDoubleValues(Object exVal, Object actVal) {
        Double expectedV = getDoubleValue(exVal);
        Double actualV = (Double) convertValue(actVal);
        if (expectedV == null || expectedV.equals(actualV)) {
            if (expectedV == null && actualV != null) {
                assertEquals(0.0, actualV);
            } else {
                assertEquals(expectedV, actualV);
            }
        } else {
            assertTrue(0.000_000_000_001 >= expectedV - actualV);
        }
    }

    private Object convertByteValues(Object o) {
        if (o instanceof Object[]) {
            if (o instanceof UnsignedByte[]) {
                o = convertBytes((UnsignedByte[]) o);
            }
            Object[] bytes = (Object[]) o;
            if (isBooleanArray(bytes)) return o;
            Boolean[] result = new Boolean[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == null || (Byte) bytes[i] == -1) {
                    result[i] = null;
                } else {
                    result[i] = (Byte) bytes[i] == 1;
                }
            }
            return result;
        }
        return o;
    }

    private static boolean isBooleanArray(Object[] bytes) {
        return Arrays.stream(bytes).filter(Objects::nonNull)
                .findFirst().get().getClass().equals(Boolean.class);
    }

    private Double getDoubleValue(Object value) {
        if (value == null) return null;
        return getBigDecimal(value).doubleValue();
    }

    private BigDecimal getBigDecimal(Object value) {
        if (value == null) return null;
        return BigDecimal.valueOf((Double) value).setScale(12, RoundingMode.DOWN);
    }

    private Object getBigDecimalArr(Object value) {
        if (value == null) return null;
        if (value.getClass().isArray()) {
            Object[] arr = (Object[]) value;
            BigDecimal[] result = new BigDecimal[arr.length];
            for (int i = 0; i < arr.length; i++) {
                result[i] = getBigDecimal(arr[i]);
            }
            return result;
        }
        return getBigDecimal(value);

    }

    private static String covertBinary(Object value) {
        if (value == null) return null;
        return new String((byte[]) value, StandardCharsets.UTF_8);
    }

    private Object convertValue(Object value) {
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).doubleValue();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else if (value instanceof UnsignedByte) {
            return ((UnsignedByte) value).byteValue() == 1;
        }
        return value;
    }

    private Object convertTimestampArray(Object timestamp) throws ParseException {
        if (timestamp == null) {
            return null;
        }
        if (timestamp.getClass().isArray()) {
            Object[] timestampArr = (Object[]) timestamp;
            Object[] result = new Object[timestampArr.length];
            for (int i = 0; i < timestampArr.length; i++) {
                result[i] = convertTimestamp(timestampArr[i]);
            }
            return result;
        } else {
            return convertTimestamp(timestamp);
        }
    }

    private Long convertTimestamp(Object timestamp) throws ParseException {
        if (timestamp == null) {
            return null;
        }
        String str = (String) timestamp;
        if (str.length() == 22) {
            str = str + "0";
        } else if (str.length() == 21) {
            str = str + "00";
        } else if (str.length() == 20) {
            str = str + "000";
        } else if (str.length() == 19) {
            str = str + ".000";
        }
        return getTimestamp(str);
    }

    private MessageReader2 createReader(String fileName) throws URISyntaxException, IOException {
        File sourceFile = new File(
                Objects.requireNonNull(FullStreamTests.class.getClassLoader().getResource(fileName)).toURI());
        return new MessageReader2(
                new FileInputStream(sourceFile), sourceFile.length(), true, 1 << 20, null);
    }

    protected Pair<DXTickStream, TableDeclaration> loadAndReplicateData(final MessageReader2 reader, int count, ColumnNamingScheme scheme) {
        DXTickStream stream = loadData(reader);
        TableDeclaration tableDeclaration = getTableDeclaration(stream, scheme);
        startStreamReplication(stream, tableDeclaration.getTableIdentity(), count, 10000, 10000, null);

        return Pair.of(stream, tableDeclaration);
    }

    private Pair<DXTickStream, TableDeclaration> loadAndReplicateQueryData(MessageReader2 reader, int count, ColumnNamingScheme scheme) {
        DXTickStream stream = loadData(reader);
        TableDeclaration tableDeclaration = getTableDeclaration(stream, scheme);
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setQuery("select * from \"" + stream.getKey() + "\"");
        queryRequest.setKey(tableDeclaration.getTableIdentity().getTableName());
        queryRequest.setTable(tableDeclaration.getTableIdentity().getTableName());
        queryRequest.setColumnNamingScheme(scheme);
        queryRequest.setWriteMode(WriteMode.APPEND);
        queryRequest.setIncludePartitionColumn(true);
        startQueryReplication(tableDeclaration.getTableIdentity(), count, 10000, 10000, queryRequest);

        return Pair.of(stream, tableDeclaration);
    }

    private DXTickStream loadData(MessageReader2 reader) {
        String streamKey = String.format("%s_%s", reader.getTypes()[0].getName(), Instant.now());
        DXTickStream stream = createStream(streamKey, reader.getTypes());
        loadStreamFromQsmsg(reader, stream);
        return stream;
    }

    private DXTickStream createStream(String streamKey, RecordClassDescriptor[] types) {
        DXTickStream sourceStream = tickDB.getStream(streamKey);
        if (sourceStream != null) sourceStream.delete();
        StreamOptions streamOptions = new StreamOptions();
        streamOptions.setPolymorphic(types);
        streamOptions.name = streamKey;
        streamOptions.periodicity = Periodicity.mkIrregular();
        sourceStream = tickDB.createStream(streamKey, streamOptions);
        return sourceStream;
    }

    private void loadStreamFromQsmsg(MessageReader2 reader, DXTickStream sourceStream) {
        try (TickLoader loader = sourceStream.createLoader(new LoadingOptions(true))) {
            while (reader.next()) {
                loader.send(reader.getMessage());
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}