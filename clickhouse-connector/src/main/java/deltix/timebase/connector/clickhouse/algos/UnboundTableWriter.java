package deltix.timebase.connector.clickhouse.algos;

import deltix.clickhouse.ClickhouseClient;
import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.clickhouse.schema.types.*;
import deltix.clickhouse.util.CheckedConsumer;
import deltix.clickhouse.util.SqlQueryHelper;
import deltix.dfp.Decimal64Utils;
import deltix.gflog.Log;
import deltix.gflog.LogEntry;
import deltix.gflog.LogFactory;
import deltix.qsrv.hf.pub.NullValueException;
import deltix.qsrv.hf.pub.RawMessage;
import deltix.qsrv.hf.pub.ReadableValue;
import deltix.qsrv.hf.pub.codec.NonStaticFieldInfo;
import deltix.qsrv.hf.pub.codec.RecordClassInfo;
import deltix.qsrv.hf.pub.codec.UnboundDecoder;
import deltix.qsrv.hf.pub.md.*;
import deltix.qsrv.hf.pub.md.ArrayDataType;
import deltix.qsrv.hf.pub.md.DataType;
import deltix.qsrv.hf.pub.md.DateTimeDataType;
import deltix.qsrv.hf.tickdb.pub.query.TypedMessageSource;
import deltix.timebase.connector.clickhouse.functional.CheckedBiConsumer;
import deltix.timebase.connector.clickhouse.util.ClickhouseUtil;
import deltix.util.memory.MemoryDataInput;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.clickhouse.ClickHouseArray;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;

import java.io.Closeable;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx.getColumnsDeep;
import static deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx.getColumnsDeepForDefinition;
import static deltix.timebase.connector.clickhouse.algos.RawDecoder.readField;

import static deltix.clickhouse.writer.codec.CodecUtil.TIMESTAMP_FORMAT_MS;
import static deltix.timebase.connector.clickhouse.algos.SchemaProcessor.*;


public class UnboundTableWriter implements Closeable {


    private static final Log LOG = LogFactory.getLog(UnboundTableWriter.class);
    private final String writerName;
    private final Map<RecordClassDescriptor, List<ColumnDeclarationEx>> columnDeclarations;
    private final MemoryDataInput dataInput;
    private final Map<RecordClassDescriptor, UnboundDecoder> messageDecoders;
    private final TableDeclaration declaration;
    private final Connection clickhouseConnection;
    private List<Codec> fieldCodecs;
    private Map<RecordClassInfo, Codec> fieldCodecsByRecordClassInfo;
    private int batchMsgCount = 0;
    private long minMsgTimestamp = Long.MIN_VALUE;
    private long maxMsgTimestamp = Long.MIN_VALUE;
    private Map<String, List<ColumnDeclarationEx>> objectColumns = new HashMap<>();

    public UnboundTableWriter(
            String writerName,
            ClickhouseClient clickhouseClient,
            TableDeclaration declaration,
            Map<RecordClassDescriptor, List<ColumnDeclarationEx>> columnDeclarations,
            List<UnboundDecoder> messageDecoders,
            MemoryDataInput dataInput/*,
            int flushSize,
            long flushIntervalMs*/) throws SQLException {
        this.writerName = writerName;
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
        this.messageDecoders = messageDecoders.stream().collect(Collectors.toMap(unboundDecoder -> unboundDecoder.getClassInfo().getDescriptor(), unboundDecoder -> unboundDecoder));
        this.declaration = declaration;


        clickhouseConnection = clickhouseClient.getConnection();

        fieldCodecs = new ArrayList<>();
        fieldCodecsByRecordClassInfo = new HashMap<>();

//        flusherThread = new Thread(flusher, "TableWriter flusher thread");
//        flusherThread.start();
    }

    private static BiConsumer<TimebaseContext, FieldContext>[] buildCodecs(UnboundTableWriter writer, RecordClassInfo classInfo) {
        NonStaticFieldInfo[] nonStaticFields = classInfo.getNonStaticFields();

        BiConsumer<TimebaseContext, FieldContext>[] result = new BiConsumer[nonStaticFields.length];

        for (int i = 0; i < nonStaticFields.length; ++i) {
            NonStaticFieldInfo dataField = nonStaticFields[i];
            CheckedBiConsumer<TimebaseContext, FieldContext, SQLException> codec;

            final DataType tbDataType = dataField.getType();

            if (tbDataType instanceof IntegerDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (rv.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.INTEGER);
                    else {
                        IntegerDataType tbIntegerDataType = (IntegerDataType) tbDataType;
                        if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT8))
                            chContext.statement.setByte(fieldContext.columnDeclaration.getStatementIndex(), (byte) rv.getInt());
                        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT16))
                            chContext.statement.setShort(fieldContext.columnDeclaration.getStatementIndex(), (short) rv.getInt());
                        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT32))
                            chContext.statement.setInt(fieldContext.columnDeclaration.getStatementIndex(), rv.getInt());
                        else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT64))
                            chContext.statement.setLong(fieldContext.columnDeclaration.getStatementIndex(), rv.getLong());
                        else
                            throw new UnsupportedOperationException(String.format("Unexpected encoding %s for IntegerDataType", tbIntegerDataType.getEncoding()));
                    }
                };
            } else if (tbDataType instanceof FloatDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (rv.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.FLOAT);
                    else {
                        FloatDataType tbFloatDataType = (FloatDataType) tbDataType;

                        switch (tbFloatDataType.getScale()) {
                            case FloatDataType.FIXED_FLOAT:
                                chContext.statement.setFloat(fieldContext.columnDeclaration.getStatementIndex(), rv.getFloat());
                                break;
                            case FloatDataType.FIXED_DOUBLE:
                            case FloatDataType.SCALE_AUTO:
                                chContext.statement.setDouble(fieldContext.columnDeclaration.getStatementIndex(), rv.getDouble());
                                break;
                            case FloatDataType.SCALE_DECIMAL64:
                                long decimal64Long = rv.getLong();
                                if (decimal64Long == Decimal64Utils.MAX_VALUE || decimal64Long == Decimal64Utils.POSITIVE_INFINITY) {
                                    chContext.statement.setBigDecimal(fieldContext.columnDeclaration.getStatementIndex(), ClickhouseUtil.DECIMAL_128_MAX_VALUE);
                                } else if (decimal64Long == Decimal64Utils.MIN_VALUE || decimal64Long == Decimal64Utils.NEGATIVE_INFINITY) {
                                    chContext.statement.setBigDecimal(fieldContext.columnDeclaration.getStatementIndex(), ClickhouseUtil.DECIMAL_128_MIN_VALUE);
                                }
                                //todo support
/*                                else if (decimal64Long == Decimal64Utils.NaN) {
                                }*/
                                else {
                                    chContext.statement.setBigDecimal(fieldContext.columnDeclaration.getStatementIndex(), new BigDecimal(Decimal64Utils.toString(rv.getLong())));
                                }
                                break;

                            default:
                                chContext.statement.setDouble(fieldContext.columnDeclaration.getStatementIndex(), rv.getDouble());
                        }
                    }
                };
            } else if (tbDataType instanceof DateTimeDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
                    else
                        // @MS A work-around since clickhouse client (0.2.4) does not support the millisecond resolution for timestamps.
                        // Simply serialize as String.
                        // chContext.statement.setTimestamp(fieldContext.columnDeclaration.getStatementIndex(), new Timestamp(tbContext.messageDecoder.getLong()));
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), TIMESTAMP_FORMAT_MS.format(new java.util.Date(tbContext.messageDecoder.getLong())));
                };
            } else if (tbDataType instanceof TimeOfDayDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.INTEGER);
                    else
                        chContext.statement.setInt(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getInt());
                };
            } else if (tbDataType instanceof VarcharDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                };
            } else if (tbDataType instanceof EnumDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                };
            } else if (tbDataType instanceof CharDataType) {
                codec = (TimebaseContext tbContext, FieldContext fieldContext) -> {
                    final UnboundDecoder rv = tbContext.messageDecoder;
                    final ClickhouseContext chContext = fieldContext.clickhouseContext;
                    if (tbContext.messageDecoder.isNull())
                        chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.VARCHAR);
                    else
                        chContext.statement.setString(fieldContext.columnDeclaration.getStatementIndex(), tbContext.messageDecoder.getString());
                    // @PH: in this case, the bytes are formatted by the CH-jdbc driver in hexadecimal format that are not human readable
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

                    ArrayDataType type = (ArrayDataType) tbDataType;
                    final DataType elementType = type.getElementDataType();
                    final boolean isClassDataType = (elementType instanceof ClassDataType);
                    final NestedDataType dataType = (NestedDataType) fieldContext.columnDeclaration.getDbDataType();

                    final List<ColumnDeclarationEx> columnsEx = dataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList());

                    if (tbContext.messageDecoder.isNull()) {
                        if (isClassDataType) {
                            for (ColumnDeclaration c : columnsEx) {
                                ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                                Object defaultValue = ce.getDefaultValue();
                                chContext.statement.setArray(ce.getStatementIndex(), new ClickHouseArray(ce.getClickHouseDataType(), new Object[]{}));
                            }
                        } else
                            chContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.ARRAY);
                    } else {
                        UnboundDecoder udec = tbContext.messageDecoder;
                        final int len = udec.getArrayLength();


                        if (isClassDataType) {

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
                                        final String columnName = SchemaProcessor.getColumnName(descriptor, field);
                                        objects = values.get(name2ColumnIndex.get(columnName));
                                        objects.set(objects.size() - 1, fieldValue);
                                    }

                                } catch (NullValueException e) {
                                }

                            }


                            for (int i1 = 0; i1 < columnsEx.size(); i1++) {
                                ColumnDeclaration c = columnsEx.get(i1);
                                ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                                chContext.statement.setArray(ce.getStatementIndex(), new ClickHouseArray(ce.getClickHouseDataType(),
                                        values.get(i1).toArray()));
                            }

                        } else {
                            final boolean isNullableBool = (elementType instanceof BooleanDataType) && elementType.isNullable();
                            Object[] values = new Object[len];

                            for (int ii = 0; ii < len; ii++) {
                                Object value;
                                try {
                                    final ReadableValue rv = udec.nextReadableElement();
                                    value = readField(elementType, rv);
                                } catch (NullValueException e) {
                                    value = null;
                                }

                                if (isNullableBool) {
                                    Boolean b = (Boolean) value;
                                    values[ii] = (byte) (b == null ? -1 : b ? 1 : 0);
                                } else {
                                    values[ii] = value;
                                }

                            }
                            chContext.statement.setArray(fieldContext.columnDeclaration.getStatementIndex(), new ClickHouseArray(convertTimebaseDataTypeToRawClickhouseDataType(elementType), values));
                        }
                    }
                };
            } else if (tbDataType instanceof ClassDataType) {

                codec = (TimebaseContext timebaseContext, FieldContext fieldContext) -> {
                    final ClickhouseContext clickhouseContext = fieldContext.clickhouseContext;

                    final DataType elementType = (ClassDataType) tbDataType;
                    final boolean isClassDataType = (elementType instanceof ClassDataType);

                    if (timebaseContext.messageDecoder.isNull()) {
                        fieldContext.columnDeclaration.writeNull.accept(clickhouseContext);
                    } else {
                        UnboundDecoder udec = timebaseContext.messageDecoder;

                        if (isClassDataType) {

                            final UnboundDecoder decoder = udec.getFieldDecoder();//rv.getFieldDecoder();
                            final RecordClassInfo info = decoder.getClassInfo();


                            final String key = udec.getClassInfo().getDescriptor().toString() + "_/\\_" + fieldContext.columnDeclaration.getDbColumnName() + "_/\\_" + info.getDescriptor().toString();
                            List<ColumnDeclarationEx> columnsEx2 = writer.objectColumns.get(key);

                            if (columnsEx2 == null) {
                                ObjectDataType objectDataType = (ObjectDataType) fieldContext.columnDeclaration.getDbDataType();
                                final List<ColumnDeclarationEx> columnsEx = objectDataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList());
                                columnsEx2 = writer.columnDeclarations.get(info.getDescriptor()).stream().map(c -> c.deepCopy()).collect(Collectors.toList());
                                setStatementIndex(columnsEx, columnsEx2);
                                writer.objectColumns.put(key, columnsEx2);
                            }

                            Codec fieldCodec = writer.fieldCodecsByRecordClassInfo.get(info);
                            if (fieldCodec == null) {
                                fieldCodec = new Codec(writer.clickhouseConnection, writer.declaration, decoder, buildCodecs(writer, info), null/*columnsEx*/, clickhouseContext.statement);
                                writer.fieldCodecsByRecordClassInfo.put(decoder.getClassInfo(), fieldCodec);
                            }

                            fieldCodec.getTimebaseContext().messageDecoder = decoder;

                            clickhouseContext.statement.setString(columnsEx2.get(SchemaProcessor.FIXED_COLUMN_NAMES.length - 1).getStatementIndex(), info.getDescriptor().getName());

                            int fieldIndex = 0;
                            while (decoder.nextField()) {
                                fieldCodec.getFieldCodecs()[fieldIndex].accept(fieldCodec.getTimebaseContext(), new FieldContext(clickhouseContext, columnsEx2.get(fieldIndex + SchemaProcessor.FIXED_COLUMN_NAMES.length)));
                                fieldIndex++;
                            }
                        }
                    }
                };
            } else {
                throw new NotImplementedException();
            }

            result[i] = wrapCodecException(codec);
        }

        return result;
    }

    private static void setStatementIndex(List<ColumnDeclarationEx> sourceAllColumns, List<ColumnDeclarationEx> destinationColumns) {
        final Map<String, ColumnDeclarationEx> allColumns = sourceAllColumns.stream().collect(Collectors.toMap(c -> c.getDbColumnName(), c -> c));
        for (ColumnDeclarationEx column : destinationColumns) {
            final ColumnDeclarationEx oc = allColumns.get(column.getDbColumnName());
            if (oc == null)
                continue;
            column.setStatementIndex(oc.getStatementIndex());

            final deltix.clickhouse.schema.types.DataType destDataType = column.getDbDataType();
            if (destDataType instanceof ObjectDataType) {
                ObjectDataType destObjectDataType = (ObjectDataType) destDataType;
                ObjectDataType srcObjectDataType = (ObjectDataType) oc.getDbDataType();

                setStatementIndex(srcObjectDataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList()),
                        destObjectDataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList()));
            } else if (destDataType instanceof NestedDataType) {
                NestedDataType destObjectDataType = (NestedDataType) destDataType;
                NestedDataType srcObjectDataType = (NestedDataType) oc.getDbDataType();
                setStatementIndex(srcObjectDataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList()),
                        destObjectDataType.getColumns().stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList()));
            }
        }
    }


    public static Consumer<ClickhouseContext> buildWriteNull(ColumnDeclarationEx columnDeclaration) {
        deltix.clickhouse.schema.types.DataType dataType = columnDeclaration.getDbDataType();
        boolean isNullable = false;
        if (dataType instanceof NullableDataType) {
            dataType = ((NullableDataType) dataType).getNestedType();
            isNullable = false;
        }
        boolean finalIsNullable = isNullable;

        CheckedConsumer<ClickhouseContext> writeNullCodec;

        if (dataType instanceof NullableDataType) {
            throw new UnsupportedOperationException();
        } else if (dataType instanceof deltix.clickhouse.schema.types.ArrayDataType) {
            throw new UnsupportedOperationException(String.format("Not supported data type '%s'", dataType.getClass().getName()));
//            writeNullCodec = clickhouseContext -> {
//                if (isClassDataType) {
//                    for (ColumnDeclaration c : columnsEx) {
//                        ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
//                        Object defaultValue = ce.getDefaultValue();
//                        clickhouseContext.statement.setArray(fieldContext.columnDeclaration.getStatementIndex(), new ClickHouseArray(ce.getClickHouseDataType(), new Object[]{}));
//                    }
//                } else
//                    clickhouseContext.statement.setNull(fieldContext.columnDeclaration.getStatementIndex(), Types.ARRAY);
//            };
//        } else if (dataType instanceof BinaryDataType) {
//            essentialDataType = ClickHouseDataType.String;
        } else if (dataType instanceof UInt8DataType) {
            writeNullCodec = clickhouseContext -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
            };
        } else if (dataType instanceof StringDataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.VARCHAR);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.VARCHAR);
            };
        } else if (dataType instanceof DateDataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.TIMESTAMP);
            };
        } else if (dataType instanceof DateTime64DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.TIMESTAMP);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.TIMESTAMP);
            };
        } else if (dataType instanceof Enum16DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.VARCHAR);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.VARCHAR);
            };
        } else if (dataType instanceof Float32DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.FLOAT);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.FLOAT);
            };
        } else if (dataType instanceof Float64DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.FLOAT);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.FLOAT);
            };
        } else if (dataType instanceof Decimal128DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.FLOAT);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.FLOAT);
            };
        } else if (dataType instanceof Int8DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
            };
        } else if (dataType instanceof Int16DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
            };
        } else if (dataType instanceof Int32DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
            };
        } else if (dataType instanceof Int64DataType) {
            writeNullCodec = (clickhouseContext) -> {
                if (finalIsNullable)
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.INTEGER);
                else
                    clickhouseContext.statement.setObject(columnDeclaration.getStatementIndex(), columnDeclaration.getDefaultValue(), Types.INTEGER);
            };
        } else if (dataType instanceof NestedDataType) {
            NestedDataType nestedDataType = (NestedDataType) dataType;
            writeNullCodec = (clickhouseContext) -> {
                boolean isClassDataType = true;
                if (isClassDataType) {
                    for (ColumnDeclaration c : nestedDataType.getColumns()) {
                        ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                        Object defaultValue = ce.getDefaultValue();
                        clickhouseContext.statement.setArray(ce.getStatementIndex(), new ClickHouseArray(ce.getClickHouseDataType(), new Object[]{}));
                    }
                } else
                    clickhouseContext.statement.setNull(columnDeclaration.getStatementIndex(), Types.ARRAY);
            };
        } else if (dataType instanceof ObjectDataType) {
            ObjectDataType nestedDataType = (ObjectDataType) dataType;
            writeNullCodec = (clickhouseContext) -> {
                for (ColumnDeclaration c : nestedDataType.getColumns()) {
                    ColumnDeclarationEx ce = (ColumnDeclarationEx) c;
                    ce.writeNull.accept(clickhouseContext);
                }
            };
        } else {
            throw new UnsupportedOperationException(String.format("Not supported data type '%s'", dataType.getClass().getName()));
        }

        return wrapCodecException(writeNullCodec);
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

    private static void encode(RawMessage message, MemoryDataInput dataInput, Codec codec
    ) throws SQLException {
        dataInput.setBytes(message.data, message.offset, message.length);
        UnboundDecoder messageDecoder = codec.getUnboundDecoder();
        BiConsumer<TimebaseContext, FieldContext>[] fieldCodecs = codec.getFieldCodecs();
        messageDecoder.beginRead(dataInput);

        int parameterIndex = 1;

        long millisTime = message.getTimeStampMs(); // TODO: switch to nanos
        CharSequence symbol = message.getSymbol();

        // TODO: heavily allocates
        PreparedStatement statement = codec.getInsertStatement();
        statement.setDate(parameterIndex++, new Date(millisTime));
        statement.setTimestamp(parameterIndex++, new Timestamp(millisTime));
        statement.setString(parameterIndex++, symbol.toString());
        statement.setString(parameterIndex++, message.type.getName());

        TimebaseContext timebaseContext = codec.getTimebaseContext();
        timebaseContext.messageDecoder = messageDecoder;
        ClickhouseContext clickhouseContext = codec.getClickhouseContext();

        clickhouseContext.statement = statement;
        //fieldContext.columnDeclaration.getStatementIndex() = parameterIndex;

        int fieldIndex = 0;
        while (messageDecoder.nextField()) {
            fieldCodecs[fieldIndex].accept(timebaseContext, new FieldContext(clickhouseContext, codec.clickhouseContext.columnDeclarations.get(fieldIndex + SchemaProcessor.FIXED_COLUMN_NAMES.length)));
            fieldIndex++;
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

//        allColumns.forEach(column -> column.writeNull.apply(clickhouseContext));

        if (codec == null) {
            UnboundDecoder unboundDecoder = messageDecoders.get(info.getCurrentType());
            codec = new Codec(clickhouseConnection, declaration, unboundDecoder, buildCodecs(this, unboundDecoder.getClassInfo()), columnDeclarations.get(message.type), null);
            fieldCodecs.set(typeIndex, codec);
            fieldCodecsByRecordClassInfo.put(unboundDecoder.getClassInfo(), codec);
        }

        final ClickhouseContext clickhouseContext = codec.clickhouseContext;
        codec.getClickhouseContext().columnDeclarations.forEach(column -> column.writeNull.accept(clickhouseContext));
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
                    ClickHousePreparedStatementImpl statement = (ClickHousePreparedStatementImpl) codec.getInsertStatement();
                    LOG.error("Failed query: " + statement.asSql());
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

        info().
                append("Closing.")
                .commit();

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
        private final TableDeclaration declaration;
        UnboundDecoder unboundDecoder;
        BiConsumer<TimebaseContext, FieldContext>[] fieldCodecs;
        TimebaseContext timebaseContext = new TimebaseContext();
        private String insertIntoQuery;

        public Codec(Connection clickhouseConnection, TableDeclaration declaration, UnboundDecoder unboundDecoder, BiConsumer<TimebaseContext, FieldContext>[] fieldCodecs, List<ColumnDeclarationEx> columnDeclarations, PreparedStatement insertStatement) throws SQLException {
            this.declaration = declaration;
            this.unboundDecoder = unboundDecoder;
            this.fieldCodecs = fieldCodecs;
            final List<ColumnDeclarationEx> columns;
            final List<ColumnDeclarationEx> columnsDeep;
            if (insertStatement == null) {
                columns = columnDeclarations.stream().map(c -> ((ColumnDeclarationEx) c).deepCopy()).collect(Collectors.toList());
                columnsDeep = getColumnsDeep(columns);
                // set statement index
                int statementIndex = 1;
                for (ColumnDeclarationEx column : columnsDeep) {
                    column.setStatementIndex(statementIndex++);
                }
                insertIntoQuery = SqlQueryHelper.getInsertIntoQuery(declaration.getTableIdentity(), getColumnsDeepForDefinition(columnDeclarations));
                this.insertStatement = clickhouseConnection.prepareStatement(insertIntoQuery);
//            if (insertStatement == null) {
//                columns = columnDeclarations;
//                insertIntoQuery = SqlQueryHelper.getInsertIntoQuery(declaration.getTableIdentity(), getColumnsDeepForDefinition(columnDeclarations));
//                this.insertStatement = clickhouseConnection.prepareStatement(insertIntoQuery);
            } else {
                columns = columnDeclarations;
                this.insertStatement = insertStatement;
            }
            clickhouseContext = new ClickhouseContext(columns);
            clickhouseContext.statement = getInsertStatement();
        }


        public PreparedStatement getInsertStatement() {
            return insertStatement;
        }

        public UnboundDecoder getUnboundDecoder() {
            return unboundDecoder;
        }

        public BiConsumer<TimebaseContext, FieldContext>[] getFieldCodecs() {
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
        private ColumnDeclarationEx columnDeclaration;
        private ClickhouseContext clickhouseContext;

        public FieldContext(ClickhouseContext clickhouseContext, ColumnDeclarationEx columnDeclaration) {
            this.clickhouseContext = clickhouseContext;
            this.columnDeclaration = columnDeclaration;
        }
    }

    static class ClickhouseContext {
        //        int parameterIndex;
        PreparedStatement statement;
        private List<ColumnDeclarationEx> columnDeclarations;

        public ClickhouseContext(List<ColumnDeclarationEx> columnDeclarations) {
            this.columnDeclarations = columnDeclarations;
        }
    }

    private static class TimebaseContext {
        private UnboundDecoder messageDecoder;
    }
}
