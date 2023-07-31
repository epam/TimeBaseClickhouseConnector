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

import com.clickhouse.data.value.ClickHouseArrayValue;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.models.TableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.clickhouse.util.CheckedConsumer;
import com.epam.deltix.clickhouse.util.SqlQueryHelper;
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogEntry;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.qsrv.hf.pub.NullValueException;
import com.epam.deltix.qsrv.hf.pub.RawMessage;
import com.epam.deltix.qsrv.hf.pub.ReadableValue;
import com.epam.deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import com.epam.deltix.qsrv.hf.pub.codec.RecordClassInfo;
import com.epam.deltix.qsrv.hf.pub.codec.RecordLayout;
import com.epam.deltix.qsrv.hf.pub.codec.UnboundDecoder;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.pub.md.ArrayDataType;
import com.epam.deltix.qsrv.hf.pub.md.DateTimeDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.query.TypedMessageSource;
import com.epam.deltix.timebase.connector.clickhouse.functional.CheckedBiConsumer;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.util.ClickhouseUtil;
import com.epam.deltix.util.memory.MemoryDataInput;

import java.io.Closeable;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.epam.deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx.getColumnsDeepForDefinition;
import static com.epam.deltix.timebase.connector.clickhouse.algos.RawDecoder.readField;

import static com.epam.deltix.timebase.connector.clickhouse.algos.Replicator.ALL_TYPES;

public class UnboundTableWriter implements Closeable {

    private static final Log LOG = LogFactory.getLog(UnboundTableWriter.class);
    private final String writerName;
    private final ColumnNamingScheme columnNamingScheme;
    private final Map<String, List<ColumnDeclarationEx>> columnDeclarations;
    //    private final ClickhouseClientSettings settings;
    //private final BlockingQueue<T> messages;
    //private final UnboundTableWriter.Flusher flusher = new UnboundTableWriter.Flusher();
    //private final Thread flusherThread;
    private final MemoryDataInput dataInput;
    //    private static final UUID WRITER_ID = UUID.randomUUID();
    private final Map<String, UnboundDecoder> messageDecoders;
    private final Map<String, TableDeclaration> declarations;
    //    private int flushSize;
//    private long flushIntervalMs;
    // temp
    private final Connection clickhouseConnection;
    //    private final ClickhouseContext clickhouseContext;
    private final List<Codec> fieldCodecs;

    private final Set<String> fixedColumnNames = new HashSet<>(List.of(SchemaProcessor.FIXED_COLUMN_NAMES));
    private final Map<RecordClassInfo, Codec> fieldCodecsByRecordClassInfo;

    //    private final ClickhouseContext clickhouseContext = new ClickhouseContext();
//    private final TimebaseContext timebaseContext = new TimebaseContext();
    //private volatile boolean closing = false;
    private int batchMsgCount = 0;
    private long minMsgTimestamp = Long.MIN_VALUE;
    private long maxMsgTimestamp = Long.MIN_VALUE;

    public UnboundTableWriter(
            String writerName,
            ColumnNamingScheme columnNamingScheme,
            ClickhouseClient clickhouseClient,
            Map<String, TableDeclaration> declarations,
            Map<String, List<ColumnDeclarationEx>> columnDeclarations,
            List<UnboundDecoder> messageDecoders,
            MemoryDataInput dataInput/*,
            int flushSize,
            long flushIntervalMs*/) throws SQLException {
        this.writerName = writerName;
        this.columnNamingScheme = columnNamingScheme;
        this.columnDeclarations = columnDeclarations;

        debug()
                .append("initializing.")
                .commit();


//        this.flushSize = flushSize;
//        this.flushIntervalMs = flushIntervalMs;

        //messages = new ArrayBlockingQueue<>(messageQueueCapacity);
//        this.clickhouseClient = clickhouseClient; // new ClickhouseClient(dataSource);
//        this.settings = clickhouseClient.getSettings();
        this.dataInput = dataInput;
        this.messageDecoders = messageDecoders.stream().collect(Collectors.toMap(unboundDecoder ->
                unboundDecoder.getClassInfo().getDescriptor().getName(), unboundDecoder -> unboundDecoder));
        this.declarations = declarations;


        clickhouseConnection = clickhouseClient.getConnection();

        fieldCodecs = new ArrayList<>();
        fieldCodecsByRecordClassInfo = new HashMap<>();

//        flusherThread = new Thread(flusher, "TableWriter flusher thread");
//        flusherThread.start();
    }

    private Map<String, BiConsumer<TimebaseContext, FieldContext>> buildCodecs(UnboundTableWriter writer, RecordClassInfo classInfo, Set<String> processColumnNames) {
        NonStaticFieldInfo[] nonStaticFields = classInfo.getNonStaticFields();

        Map<String, BiConsumer<TimebaseContext, FieldContext>> result = new HashMap<>();
        for (int i = 0; i < nonStaticFields.length; ++i) {
            NonStaticFieldInfo dataField = nonStaticFields[i];

            String fieldAsColumnName = SchemaProcessor.getColumnName(classInfo.getDescriptor(), dataField, columnNamingScheme);
            if (!processColumnNames.contains(fieldAsColumnName))
                continue;

            CheckedBiConsumer<TimebaseContext, FieldContext, SQLException> codec;

            final com.epam.deltix.qsrv.hf.pub.md.DataType tbDataType = dataField.getType();

            if (tbDataType instanceof IntegerDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (rv.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.INTEGER);
                    else {
                        IntegerDataType tbIntegerDataType = (IntegerDataType) tbDataType;
                        switch (tbIntegerDataType.getNativeTypeSize()) {
                            case 1:
                                chContext.statement.setByte(fieldContext.columnDeclaration.getStatementIndex(), (byte) rv.getInt());
                                break;
                            case 2:
                                chContext.statement.setShort(fieldContext.columnDeclaration.getStatementIndex(), (short) rv.getInt());
                                break;
                            case 4:
                                chContext.statement.setInt(fieldContext.columnDeclaration.getStatementIndex(), rv.getInt());
                                break;
                            default:
                                chContext.statement.setLong(fieldContext.columnDeclaration.getStatementIndex(), rv.getLong());
                        }
                    }
                };
            } else if (tbDataType instanceof FloatDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    int statementIndex = fieldContext.columnDeclaration.getStatementIndex();

                    if (rv.isNull())
                        chContext.statement.setNull(statementIndex, Types.FLOAT);
                    else {
                        FloatDataType tbFloatDataType = (FloatDataType) tbDataType;

                        switch (tbFloatDataType.getScale()) {
                            case FloatDataType.FIXED_FLOAT:
                                chContext.statement.setFloat(statementIndex, rv.getFloat());
                                break;
                            case FloatDataType.FIXED_DOUBLE:
                            case FloatDataType.SCALE_AUTO:
                                chContext.statement.setDouble(statementIndex, rv.getDouble());
                                break;
                            case FloatDataType.SCALE_DECIMAL64:
                                long decimal64Long = rv.getLong();
                                if (decimal64Long == Decimal64Utils.MAX_VALUE || decimal64Long == Decimal64Utils.POSITIVE_INFINITY) {
                                    chContext.statement.setBigDecimal(statementIndex, ClickhouseUtil.DECIMAL_128_MAX_VALUE);
                                } else if (decimal64Long == Decimal64Utils.MIN_VALUE || decimal64Long == Decimal64Utils.NEGATIVE_INFINITY) {
                                    chContext.statement.setBigDecimal(statementIndex, ClickhouseUtil.DECIMAL_128_MIN_VALUE);
                                }
                                //todo support
/*                                else if (decimal64Long == Decimal64Utils.NaN) {
                                }*/
                                else {
                                    chContext.statement.setBigDecimal(statementIndex, new BigDecimal(Decimal64Utils.toString(rv.getLong())));
                                }
                                break;

                            default:
                                chContext.statement.setDouble(statementIndex, rv.getDouble());
                        }
                    }
                };
            } else if (tbDataType instanceof DateTimeDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;

                    if (tbContext.messageDecoder.isNull()) {
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
                    } else {
                        long value = tbContext.messageDecoder.getLong();

                        if (value > ClickhouseUtil.DATETIME_64_MAX_VALUE)
                            value = ClickhouseUtil.DATETIME_64_MAX_VALUE;
                        else if (value < ClickhouseUtil.DATETIME_64_MIN_VALUE)
                            value = ClickhouseUtil.DATETIME_64_MIN_VALUE;

                        chContext.statement.setTimestamp(fieldContext.columnDeclaration.getStatementIndex(), new Timestamp(value));
                    }
                };
            } else if (tbDataType instanceof TimeOfDayDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.INTEGER);
                    else
                        chContext.statement.setInt(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getInt());
                };
            } else if (tbDataType instanceof VarcharDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                };
            } else if (tbDataType instanceof EnumDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                };
            } else if (tbDataType instanceof CharDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                    // @PH: in this case, the bytes are formatted by the CH-jdbc driver in hexadecimal format that are not human-readable
                    //chContext.statement.setBytes(fieldContext.columnDeclaration.getStatementIndex(), new byte[] { (byte) tbContext.messageDecoder.getChar() });
                };
            } else if (tbDataType instanceof BooleanDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.BOOLEAN);
                    else
                        chContext.statement.setBoolean(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getBoolean());
                };
            } else if (tbDataType instanceof BinaryDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.BINARY);
                    else {
                        int binaryLength = tbContext.messageDecoder.getBinaryLength();
                        byte[] binaryValue = new byte[binaryLength];
                        tbContext.messageDecoder.getBinary(0, binaryLength, binaryValue, 0);

                        chContext.statement.setBytes(fieldContext.columnDeclaration.getStatementIndex(), binaryValue);
                    }
                };
            } else if (tbDataType instanceof ArrayDataType) {

                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    ColumnDeclarationEx columnDeclaration = fieldContext.columnDeclaration;

                    ArrayDataType type = (ArrayDataType) tbDataType;
                    final com.epam.deltix.qsrv.hf.pub.md.DataType elementType = type.getElementDataType();
                    final boolean isClassDataType = (elementType instanceof ClassDataType);

                    if (tbContext.messageDecoder.isNull()) {
                        if (isClassDataType) {
                            setInnerNullValues(chContext, columnDeclaration, true);
                        } else
                            chContext.statement.setObject(columnDeclaration.getStatementIndex(), ClickHouseArrayValue.ofEmpty());
                    } else {
                        UnboundDecoder udec = tbContext.messageDecoder;
                        final int len = udec.getArrayLength();


                        if (isClassDataType) {
                            final NestedDataType dataType = (NestedDataType) columnDeclaration.getDbDataType();
                            final List<ColumnDeclarationEx> columnsEx = ClickhouseUtil.toExColumnsUnchecked(dataType.getColumns());

                            ArrayList<List<Object>> values = new ArrayList<>(columnsEx.size());
                            //TODO: need to optimize
                            Map<String, Integer> name2ColumnIndex = new HashMap<>();
                            for (int i2 = 0; i2 < columnsEx.size(); ++i2) {
                                values.add(new ArrayList<Object>());
                                name2ColumnIndex.put(columnsEx.get(i2).getDbColumnName(), i2);
                            }

                            for (int ii = 0; ii < len; ii++) {
                                for (int i2 = 0; i2 < values.size(); ++i2) {
                                    values.get(i2).add(columnsEx.get(i2).getDefaultValue());
                                }

                                try {
                                    final ReadableValue rv = udec.nextReadableElement();
                                    final UnboundDecoder decoder = rv.getFieldDecoder();

                                    final RecordClassDescriptor descriptor = decoder.getClassInfo().getDescriptor();
                                    List<Object> objects = values.get(0);
                                    objects.set(objects.size() - 1, descriptor.getName());

                                    // dump field/value pairs
                                    while (decoder.nextField()) {
                                        NonStaticFieldInfo field = decoder.getField();
                                        Object fieldValue = readField(field.getType(), decoder);
                                        final String columnName = tbContext.getColumnName(descriptor.getName(), field.getName());
                                        objects = values.get(name2ColumnIndex.get(columnName));
                                        objects.set(objects.size() - 1, fieldValue);
                                    }

                                } catch (NullValueException e) {
                                }

                            }

                            for (int i1 = 0; i1 < columnsEx.size(); i1++) {
                                ColumnDeclaration c = columnsEx.get(i1);
                                ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                                List<Object> objects = values.get(i1);
                                if (ce.getDbDataType() instanceof NestedDataType) {
                                    throw new UnsupportedOperationException();
                                } else if (ce.getDbDataType() instanceof ObjectDataType) {
                                    List<ColumnDeclarationEx> columns = ClickhouseUtil.toExColumnsUnchecked(((ObjectDataType) ce.getDbDataType()).getColumns());
                                    for (ColumnDeclarationEx column : columns) {
                                        Object[] valuesArray = new Object[objects.size()];
                                        for (int i2 = 0; i2 < objects.size(); i2++) {
                                            Map<String, Object> object = (Map<String, Object>) objects.get(i2);
                                            valuesArray[i2] = object.get(getFieldNameByColumn(column.getDbColumnName(), columnNamingScheme));
                                        }
                                        chContext.statement.setObject(column.getStatementIndex(), ClickHouseArrayValue.of(valuesArray));
                                    }
                                } else {
                                    chContext.statement.setObject(ce.getStatementIndex(), ClickHouseArrayValue.of(objects.toArray()));
                                }
                            }

                        } else {
                            Object[] values = new Object[len];

                            for (int ii = 0; ii < len; ii++) {
                                final ReadableValue rv = udec.nextReadableElement();
                                values[ii] = readField(elementType, rv);
                            }
                            chContext.statement.setArray(columnDeclaration.getStatementIndex(), (Array) getArrayValues(chContext, columnDeclaration, values));
                        }
                    }
                };
            } else if (tbDataType instanceof ClassDataType) {

                codec = (TimebaseContext timebaseContext, FieldContext fieldContext) -> {
                    final ClickhouseContext clickhouseContext = fieldContext.clickhouseContext;

                    if (timebaseContext.messageDecoder.isNull()) {
                        setNullableValue(clickhouseContext, fieldContext.columnDeclaration);
                    } else {
                        UnboundDecoder udec = timebaseContext.messageDecoder;

                        final UnboundDecoder decoder = udec.getFieldDecoder();
                        final RecordClassInfo info = decoder.getClassInfo();

                        ObjectDataType objectDataType = (ObjectDataType) fieldContext.columnDeclaration.getDbDataType();

                        List<ColumnDeclarationEx> columns = ClickhouseUtil.toExColumnsUnchecked(objectDataType.getColumns());
                        Set<String> availableFieldsNames = ClickhouseUtil.getAvailableFieldsNames(columns);
                        availableFieldsNames.remove(SchemaProcessor.TYPE_COLUMN_NAME);

                        Codec fieldCodec = writer.fieldCodecsByRecordClassInfo.get(info);
                        if (fieldCodec == null) {
                            fieldCodec = new Codec(writer.clickhouseConnection, null, decoder,
                                    buildCodecs(writer, info, availableFieldsNames), null/*columnsEx*/, clickhouseContext.statement);
                            writer.fieldCodecsByRecordClassInfo.put(decoder.getClassInfo(), fieldCodec);
                            fieldCodec.timebaseContext.buildMappings(info, columnNamingScheme);
                        }

                        fieldCodec.getTimebaseContext().messageDecoder = decoder;
                        TimebaseContext fieldTimebaseContext = fieldCodec.timebaseContext;
                        clickhouseContext.statement.setString(columns.get(0).getStatementIndex(), info.getDescriptor().getName());

                        while (decoder.nextField()) {
                            String columnName = fieldTimebaseContext.getColumnName(info.getDescriptor().getName(), decoder.getField().getName());
                            if (availableFieldsNames.contains(columnName)) {
                                fieldCodec.getFieldCodecs().get(columnName).accept(fieldCodec.getTimebaseContext(),
                                        new FieldContext(clickhouseContext, findColumnByName(columns, columnName)));
                            }
                        }
                    }
                };
            } else {
                throw new UnsupportedOperationException();
            }

            result.put(fieldAsColumnName, wrapCodecException(codec));
        }
        return result;
    }

    private ColumnDeclarationEx findColumnByName(List<ColumnDeclarationEx> columns, String columnName) {
        for (ColumnDeclarationEx column : columns) {
            if (column.getDbColumnName().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    private static String getFieldNameByColumn(String dbColumnName, ColumnNamingScheme columnNamingScheme) {
        int index = dbColumnName.indexOf('_');
        if (index != -1) {
            switch (columnNamingScheme) {
                case TYPE_AND_NAME:
                    return dbColumnName.substring(index + 1);
                case NAME_AND_DATATYPE:
                    return dbColumnName.substring(0, index);
            }
        }
        return dbColumnName;
    }

    private static Object getArrayValues(ClickhouseContext chContext, ColumnDeclarationEx columnDeclaration, Object[] values) throws SQLException {
        SqlDataType dbDataType = columnDeclaration.getDbDataType();
        if (dbDataType instanceof ArraySqlType) {
            String dataTypeName = ((ArraySqlType) dbDataType).getElementType().getSqlDefinition();
            return chContext.statement.getConnection().createArrayOf(dataTypeName, values);
        }
        return values;
    }

    public static void setNullableValue(ClickhouseContext clickhouseContext, ColumnDeclarationEx columnDeclaration) throws SQLException {
        SqlDataType dataType = columnDeclaration.getDbDataType();
        boolean isNullable = false;
        if (dataType instanceof NullableDataType) {
            dataType = ((NullableDataType) dataType).getNestedType();
            isNullable = true;
        }

        if (dataType instanceof NullableDataType) {
            throw new UnsupportedOperationException();
        } else if (dataType instanceof ArraySqlType) {
            clickhouseContext.statement.setArray(columnDeclaration.getStatementIndex(), (Array) getArrayValues(clickhouseContext, columnDeclaration, new Object[0]));
//        } else if (dataType instanceof BinaryDataType) {
//            essentialDataType = ClickHouseDataType.String;
        } else if (dataType instanceof UInt8DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
        } else if (dataType instanceof StringDataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.VARCHAR);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.VARCHAR);
        } else if (dataType instanceof DateDataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.TIMESTAMP);
        } else if (dataType instanceof DateTime64DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.TIMESTAMP);
        } else if (dataType instanceof Enum16DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.VARCHAR);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.VARCHAR);
        } else if (dataType instanceof Float32DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.FLOAT);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.FLOAT);
        } else if (dataType instanceof Float64DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.DOUBLE);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.DOUBLE);
        } else if (dataType instanceof DecimalDataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.DECIMAL);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.DECIMAL);
        } else if (dataType instanceof Int8DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
        } else if (dataType instanceof Int16DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
        } else if (dataType instanceof Int32DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
        } else if (dataType instanceof Int64DataType) {
            if (isNullable)
                clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.BIGINT);
            else
                clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.BIGINT);
        } else if (dataType instanceof NestedDataType) {
            setInnerNullValues(clickhouseContext, columnDeclaration, true);
        } else if (dataType instanceof ObjectDataType) {
            ObjectDataType nestedDataType = (ObjectDataType) dataType;
            for (ColumnDeclaration c : nestedDataType.getColumns()) {
                ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                setNullableValue(clickhouseContext, ce);
            }
        } else {
            throw new UnsupportedOperationException(String.format("Not supported data type '%s'", dataType.getClass().getName()));
        }
    }

    private static void setInnerNullValues(ClickhouseContext clickhouseContext, ColumnDeclarationEx cd, boolean isNested) {
        SqlDataType dbDataType = cd.getDbDataType();
        if (cd.getStatementIndex() == 0) {
            if (dbDataType instanceof NestedDataType) {
                ((NestedDataType) dbDataType).getColumns()
                        .stream()
                        .map(c -> (ColumnDeclarationEx) c)
                        .forEach(c -> setInnerNullValues(clickhouseContext, c, true));
            } else if (dbDataType instanceof ObjectDataType) {
                ((ObjectDataType) dbDataType).getColumns()
                        .stream()
                        .map(c -> (ColumnDeclarationEx) c)
                        .forEach(c -> setInnerNullValues(clickhouseContext, c, isNested));
            }
        } else {
            try {
                if (isNested) {
                    clickhouseContext.statement.setObject(cd.getStatementIndex(), null);
                } else {
                    clickhouseContext.statement.setObject(cd.getStatementIndex(), cd.getDefaultValue());
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static Consumer<ClickhouseContext> wrapCodecException(
            CheckedConsumer<ClickhouseContext> checked) {
        return (ClickhouseContext chContext) -> {
            try {
                checked.accept(chContext);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static BiConsumer<TimebaseContext, FieldContext> wrapCodecException(
            CheckedBiConsumer<TimebaseContext, FieldContext, SQLException> checked) {
        return (TimebaseContext timebaseContext, FieldContext fieldContext) -> {
            try {
                checked.accept(timebaseContext, fieldContext);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void encode(RawMessage message, MemoryDataInput dataInput, Codec codec)
            throws SQLException
    {
        dataInput.setBytes(message.data, message.offset, message.length);
        UnboundDecoder messageDecoder = codec.getUnboundDecoder();
        Map<String, BiConsumer<TimebaseContext, FieldContext>> fieldCodecs = codec.getFieldCodecs();
        messageDecoder.beginRead(dataInput);

        ClickhouseContext clickhouseContext = codec.getClickhouseContext();
        Set<String> availableColumnNames = clickhouseContext.map.keySet();

        int parameterIndex = 1;
        long millisTime = message.getTimeStampMs(); // TODO: switch to nanos
        CharSequence symbol = message.getSymbol();

        // TODO: heavily allocates
        PreparedStatement statement = codec.getInsertStatement();
        if (availableColumnNames.contains(SchemaProcessor.PARTITION_COLUMN_NAME)) {
            statement.setDate(parameterIndex++, new Date(millisTime));
        }
        statement.setTimestamp(parameterIndex++, new Timestamp(millisTime));
        statement.setString(parameterIndex++, symbol.toString());
        statement.setString(parameterIndex++, message.type.getName());

        TimebaseContext timebaseContext = codec.getTimebaseContext();
        timebaseContext.messageDecoder = messageDecoder;
        clickhouseContext.statement = statement;
        //fieldContext.columnDeclaration.getStatementIndex() = parameterIndex;

        while (messageDecoder.nextField()) {
            String columnName = timebaseContext.getColumnName(message.type.getName(), messageDecoder.getField().getName());
            if (availableColumnNames.contains(columnName)) {
                fieldCodecs.get(columnName).accept(timebaseContext,
                        new FieldContext(clickhouseContext, codec.clickhouseContext.getColumn(columnName)));
            }
        }

        statement.addBatch();
    }

    public int getBatchMsgCount() {
        return batchMsgCount;
    }

    public long getMinMsgTimestamp() {
        return minMsgTimestamp;
    }

    public long getMaxMsgTimestamp() {
        return maxMsgTimestamp;
    }

    public final void send(RawMessage message, TypedMessageSource info) throws SQLException {
//        if (inClosing.get())
//            throw new IllegalArgumentException(String.format("Table writer `%s` closing.", writerName));

        if (message == null)
            throw new IllegalArgumentException("Message cannot be null.");

        Codec codec = null;

        int typeIndex = info.getCurrentTypeIndex();
        if (typeIndex >= fieldCodecs.size()) {
            for (int i = 0, count = typeIndex - fieldCodecs.size() + 1; i < count; ++i)
                fieldCodecs.add(null);
        }

        codec = fieldCodecs.get(typeIndex);

        if (codec == null) {
            UnboundDecoder unboundDecoder = messageDecoders.get(info.getCurrentType().getName());
            TableDeclaration declaration = declarations.containsKey(ALL_TYPES) ? declarations.get(ALL_TYPES) :
                    declarations.get(info.getCurrentType().getName());
            List<ColumnDeclarationEx> insertColumns = ClickhouseUtil.getInsertColumns(declaration, columnDeclarations.get(message.type.getName()));
            Set<String> availableFieldsNames = ClickhouseUtil.getAvailableFieldsNames(insertColumns);
            availableFieldsNames.removeAll(fixedColumnNames);

            codec = new Codec(clickhouseConnection, declaration.getTableIdentity(), unboundDecoder,
                    buildCodecs(this, unboundDecoder.getClassInfo(), availableFieldsNames), insertColumns, null);
            fieldCodecs.set(typeIndex, codec);
            fieldCodecsByRecordClassInfo.put(unboundDecoder.getClassInfo(), codec);

            codec.timebaseContext.buildMappings(unboundDecoder.getClassInfo(), columnNamingScheme);
        }

//        final ClickhouseContext clickhouseContext = codec.clickhouseContext;
//        try {
        encode(message, dataInput, codec);
        batchMsgCount++;
        if (minMsgTimestamp == Long.MIN_VALUE)
            minMsgTimestamp = message.getTimeStampMs();
        maxMsgTimestamp = message.getTimeStampMs();

        //flusher.autoResetEvent.set();

//            if (++batchMsgCount == flushSize) {
//                insertStatement.executeBatch();
//                batchMsgCount = 0;
//            }

//        } catch (SQLException ex) {
//            error()
//                    .append("message processing failed.")
//                    .append(System.lineSeparator())
//                    .append(ex)
//                    .commit();
//        }
    }

    public void flush() throws SQLException {
        if (batchMsgCount == 0)
            return;

        for (Codec codec : fieldCodecs) {
            if (codec != null)
                try {
                    codec.getInsertStatement().executeBatch();
                } catch (SQLException e) {
                    LOG.error("Execute query failed by: " + e.getMessage());
                    throw e;
                }
        }
        batchMsgCount = 0;
        minMsgTimestamp = maxMsgTimestamp = Long.MIN_VALUE;
    }

    @Override
    public void close() {
//        if (inClosing.compareAndSet(false, true)) {
//            debug()
//                    .append("closing")
//                    .commit();

        info().append("Closing.").commit();

        try {
            for (Codec codec : fieldCodecs) {
                if (codec != null) {
                    PreparedStatement insertStatement = codec.getInsertStatement();
                    if (insertStatement != null)
                        insertStatement.close();
                }
            }

            if (clickhouseConnection != null)
                clickhouseConnection.close();
        } catch (SQLException e) {
            error()
                    .append("Close operation failed.")
                    .append(e)
                    .commit();
        }

//            flusher.autoResetEvent.set();
//
//            try {
//                flusherThread.join();
//            } catch (InterruptedException e) {
//                LOG.info()
//                        .append("Flushed thread was interrupted.");
//            }
//        } else {
//            debug()
//                    .append("ignoring close request: already closing.")
//                    .commit();
//        }
    }

    public boolean removeFixedColumn(String name) {
        return fixedColumnNames.remove(name);
    }

    private LogEntry debug() {
        return LOG.debug()
                .append("Writer ")
                .append(writerName)
                .append(": ");
    }

    private LogEntry info() {
        return LOG.info()
                .append("Writer ")
                .append(writerName)
                .append(": ");
    }

    private LogEntry error() {
        return LOG.error()
                .append("Writer ")
                .append(writerName)
                .append(": ");
    }

    private static class Codec {
        private final PreparedStatement insertStatement;
        private final ClickhouseContext clickhouseContext;
        UnboundDecoder unboundDecoder;
        Map<String, BiConsumer<TimebaseContext, FieldContext>> fieldCodecs;
        final TimebaseContext timebaseContext = new TimebaseContext();
        private String insertIntoQuery;

        public Codec(Connection clickhouseConnection,
                    TableIdentity tableIdentity,
                    UnboundDecoder unboundDecoder,
                    Map<String, BiConsumer<TimebaseContext, FieldContext>> fieldCodecs,
                    List<ColumnDeclarationEx> insertColumns,
                    PreparedStatement insertStatement) throws SQLException {

            this.unboundDecoder = unboundDecoder;
            this.fieldCodecs = fieldCodecs;

            if (insertStatement == null) {
                insertIntoQuery = SqlQueryHelper.getInsertIntoQuery(tableIdentity, getColumnsDeepForDefinition(insertColumns));
                this.insertStatement = clickhouseConnection.prepareStatement(insertIntoQuery);
            } else {
                this.insertStatement = insertStatement;
            }
            clickhouseContext = new ClickhouseContext(insertColumns);
            clickhouseContext.statement = getInsertStatement();
        }


        public PreparedStatement getInsertStatement() {
            return insertStatement;
        }

        public UnboundDecoder getUnboundDecoder() {
            return unboundDecoder;
        }

        public Map<String, BiConsumer<TimebaseContext, FieldContext>> getFieldCodecs() {
            return fieldCodecs;
        }

        public String getInsertIntoQuery() {
            return insertIntoQuery;
        }

        public TimebaseContext getTimebaseContext() {
            return timebaseContext;
        }

        public ClickhouseContext getClickhouseContext() {
            return clickhouseContext;
        }
    }

    static class FieldContext {
        private final ColumnDeclarationEx columnDeclaration;
        private final ClickhouseContext clickhouseContext;

        public FieldContext(ClickhouseContext clickhouseContext, ColumnDeclarationEx columnDeclaration) {
            assert clickhouseContext != null;
            assert columnDeclaration != null;

            this.clickhouseContext = clickhouseContext;
            this.columnDeclaration = columnDeclaration;
        }
    }

    static class ClickhouseContext {
        //        int parameterIndex;
        PreparedStatement statement;
        private final List<ColumnDeclarationEx> columnDeclarations;

        private final Map<String, ColumnDeclarationEx> map = new HashMap<>();

        public ClickhouseContext(List<ColumnDeclarationEx> columnDeclarations) {
            this.columnDeclarations = columnDeclarations;
            for (int i = 0; columnDeclarations != null && i < columnDeclarations.size(); i++)
                map.put(columnDeclarations.get(i).getDbColumnName(), columnDeclarations.get(i));
        }

        public ColumnDeclarationEx getColumn(String name) {
            return map.get(name);
        }
    }

    private static class TimebaseContext {
        private final Map<String, String> mapping = new HashMap<>(); // map for fieldName -> columnName
        private UnboundDecoder messageDecoder;

        String getColumnName(String typeName, String fieldName) {
            return mapping.get(buildKey(typeName, fieldName));
        }

        void buildMappings(RecordClassInfo info, ColumnNamingScheme scheme) {
            NonStaticFieldInfo[] fields = info.getNonStaticFields();
            String typeName = info.getDescriptor().getName();
            for (int i = 0; i < fields.length; i++) {
                com.epam.deltix.qsrv.hf.pub.md.DataType type = fields[i].getType();
                if (type.getCode() == com.epam.deltix.qsrv.hf.pub.md.DataType.T_ARRAY_TYPE) {
                    DataType elementDataType = ((ArrayDataType) type).getElementDataType();
                    if (elementDataType.getCode() == DataType.T_OBJECT_TYPE) {
                        RecordClassDescriptor[] descriptors = ((ClassDataType) elementDataType).getDescriptors();
                        for (RecordClassDescriptor descriptor : descriptors) {
                            buildMappings(new RecordLayout(descriptor), scheme);
                        }
                    }
                }
                mapping.put(buildKey(typeName, fields[i].getName()), SchemaProcessor.getColumnName(info.getDescriptor(), fields[i], scheme));
            }
        }

        private String buildKey(String typeName, String fieldName) {
            return typeName + "_" + fieldName;
        }
    }
}