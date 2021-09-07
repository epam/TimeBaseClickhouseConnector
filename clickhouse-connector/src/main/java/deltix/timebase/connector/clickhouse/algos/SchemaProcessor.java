package deltix.timebase.connector.clickhouse.algos;

import com.epam.deltix.gflog.api.*;
import deltix.clickhouse.ClickhouseClient;
import deltix.clickhouse.models.ClickhouseTableIdentity;
import deltix.clickhouse.models.TableIdentity;
import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.TableDeclaration;
import deltix.clickhouse.schema.engines.MergeTreeEngine;
import deltix.clickhouse.schema.types.*;
import deltix.clickhouse.schema.types.DataType;
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.qsrv.hf.pub.codec.DataFieldInfo;
import com.epam.deltix.qsrv.hf.pub.codec.NonStaticFieldLayout;
import com.epam.deltix.qsrv.hf.pub.codec.RecordLayout;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.pub.md.ArrayDataType;
import com.epam.deltix.qsrv.hf.pub.md.DateTimeDataType;
import com.epam.deltix.qsrv.hf.tickdb.pub.TickStream;
import deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import deltix.timebase.connector.clickhouse.util.StringUtil;
import org.apache.commons.lang3.NotImplementedException;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx.getColumnsDeep;

public class SchemaProcessor {

    public static final String PARTITION_COLUMN_NAME = "partition";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String INSTRUMENT_COLUMN_NAME = "instrument";
    public static final String TYPE_COLUMN_NAME = "type";

    public static final String[] FIXED_COLUMN_NAMES = {PARTITION_COLUMN_NAME, TIMESTAMP_COLUMN_NAME, INSTRUMENT_COLUMN_NAME, TYPE_COLUMN_NAME};

    public static final String COLUMN_NAME_PART_SEPARATOR = "_";
    public static final int DEFAULT_DECIMAL_SCALE = 12;

    protected static final Log LOG = LogFactory.getLog(SchemaProcessor.class);
    private List<ColumnDeclarationEx> allColumns = new ArrayList<>();
    private final TickStream timebaseStream;
    private final ClickhouseClient clickhouseClient;
    private final ClickhouseProperties clickhouseProperties;

    private Map<RecordClassDescriptor, List<ColumnDeclarationEx>> columnDeclarations = new HashMap<>();

    public SchemaProcessor(TickStream timebaseStream,
                           ClickhouseClient clickhouseClient,
                           ClickhouseProperties clickhouseProperties) {
        this.timebaseStream = timebaseStream;
        this.clickhouseClient = clickhouseClient;
        this.clickhouseProperties = clickhouseProperties;
    }

    public static TableDeclaration  timebaseStreamToClickhouseTable(TickStream stream,
                                                                   Map<RecordClassDescriptor, List<ColumnDeclarationEx>> columnDeclarations, List<ColumnDeclarationEx> allColumns,
                                                                   String databaseName) {
        Set<RecordClassDescriptor> topLevelDescriptors = new HashSet<>();
        if (stream.isFixedType())
            topLevelDescriptors.add(stream.getFixedType());
        else
            topLevelDescriptors.addAll(Arrays.stream(stream.getPolymorphicDescriptors()).collect(Collectors.toList()));

        ClassDescriptor[] allDescriptors = stream.getAllDescriptors();
        TableIdentity tableIdentity = ClickhouseTableIdentity.of(databaseName, normalizeStreamNameToClickhouseNotation(stream.getKey()));
        Map<String,ColumnDeclarationEx> allColumnNames = new HashMap<>();

        ColumnDeclarationEx partition = new ColumnDeclarationEx(PARTITION_COLUMN_NAME,
                new DateDataType(), true, false);
        allColumns.add(partition);

        ColumnDeclarationEx timestamp = new ColumnDeclarationEx(TIMESTAMP_COLUMN_NAME,
                new DateTime64DataType(9), false, false);
        allColumns.add(timestamp);

        ColumnDeclarationEx instrument = new ColumnDeclarationEx(INSTRUMENT_COLUMN_NAME,
                new StringDataType(), false, true);
        allColumns.add(instrument);
        allColumns.add(new ColumnDeclarationEx(TYPE_COLUMN_NAME, new StringDataType(), false, true));

        ArrayList<ColumnDeclarationEx> fixedColumns = new ArrayList<>();
        fixedColumns.addAll(allColumns);

        for (ClassDescriptor classDescriptor : allDescriptors) {
            if (classDescriptor instanceof RecordClassDescriptor) {
                RecordClassDescriptor descriptor = (RecordClassDescriptor) classDescriptor;
                if (descriptor.isAbstract())
                    continue;

                RecordLayout recordLayout = new RecordLayout(descriptor);

                List<ColumnDeclarationEx> dataColumns = new ArrayList<>();
                final NonStaticFieldLayout[] nonStaticFields = recordLayout.getNonStaticFields();
                if (nonStaticFields == null)
                    continue;

                final boolean isTopLevelDescriptor = topLevelDescriptors.contains(classDescriptor);

                for (NonStaticFieldLayout dataField : nonStaticFields) {
                    ColumnDeclarationEx columnDeclarationStream = timebaseDataFieldToClickhouseColumnDeclaration(descriptor, dataField, allDescriptors);
                    dataColumns.add(columnDeclarationStream);
                }
                for (int i = 0; i < dataColumns.size(); i++) {
                    ColumnDeclarationEx column = dataColumns.get(i);
                    ColumnDeclarationEx columnProcessed = allColumnNames.get(column.getDbColumnName());
                    if (columnProcessed != null) {
                        dataColumns.set(i, columnProcessed);
                    }else {
                        if (isTopLevelDescriptor) {
                            allColumns.add(column);
                            allColumnNames.put(column.getDbColumnName(), column);
                        }
                    }
                }

                List<ColumnDeclarationEx> recordColumns = new ArrayList<>();
                recordColumns.addAll(fixedColumns);
                recordColumns.addAll(dataColumns);
                columnDeclarations.put(descriptor, recordColumns);
            }
        }

        List<ColumnDeclarationEx> columnsDeep = getColumnsDeep(allColumns);
        // set statement index
        int statementIndex = 1;
        for (ColumnDeclarationEx column : columnsDeep) {
            column.setStatementIndex(statementIndex++);
        }


        TableDeclaration tableDeclaration = new TableDeclaration(tableIdentity, allColumns.stream().map(column -> (ColumnDeclaration) column).collect(Collectors.toList()));
        return tableDeclaration;
    }

    private static String normalizeStreamNameToClickhouseNotation(String streamName) {
        if (streamName == null)
            throw new IllegalArgumentException("Stream naem cannot ne null");

        return encodeColumnName(streamName);
    }

    public static ColumnDeclarationEx timebaseDataFieldToClickhouseColumnDeclaration(RecordClassDescriptor descriptor, DataFieldInfo dataField, ClassDescriptor[] descriptors) {
        com.epam.deltix.qsrv.hf.pub.md.DataType tbDataType = dataField.getType();
        if (tbDataType instanceof ArrayDataType) {
            return columnFromArray(descriptor, dataField, descriptors);
        } else if (tbDataType instanceof ClassDataType) {
            return columnFromClass(descriptor, dataField, descriptors);
        }

        DataType dataType = convertTimebaseDataTypeToClickhouseDataType(tbDataType);
//        if (isArray)
//            dataType = new deltix.clickhouse.schema.types.ArrayDataType(dataType);
        return new ColumnDeclarationEx(getColumnName(descriptor, dataField),
                dataType);
    }

    private static ColumnDeclarationEx columnFromClass(RecordClassDescriptor descriptor, DataFieldInfo dataField, ClassDescriptor[] descriptors) {
        ClassDataType classDataType = (ClassDataType) dataField.getType();
        RecordClassDescriptor[] descriptors1 = classDataType.getDescriptors();

        List<ColumnDeclarationEx> columns = new ArrayList<>();
        columns.add(new ColumnDeclarationEx(TYPE_COLUMN_NAME, new NullableDataType(new StringDataType()), false, true));
        Set<String> columnNames = new HashSet<>();
        List<ColumnDeclarationEx> dataColumns = new ArrayList<>();
        for (RecordClassDescriptor classDescriptor : descriptors1) {
            RecordLayout recordLayout = new RecordLayout(classDescriptor);

            for (DataFieldInfo df : recordLayout.getNonStaticFields()) {
                ColumnDeclarationEx columnDeclarationStream = timebaseDataFieldToClickhouseColumnDeclaration(classDescriptor, df, descriptors/*, dataField.getName()*/);
                dataColumns.add(columnDeclarationStream);
            }

            for (ColumnDeclarationEx column : dataColumns) {
                if (columnNames.contains(column.getDbColumnName()))
                    continue;
                columns.add(column);
                columnNames.add(column.getDbColumnName());
            }
        }

        //TODO: strange workaround
        final String columnName = getColumnName(descriptor, dataField, new ObjectDataType(null, new ArrayList<>()));

        ObjectDataType dataType = new ObjectDataType(columnName, columns.stream().map(column -> (ColumnDeclaration) column).collect(Collectors.toList()));

        return new ColumnDeclarationEx(columnName,
                dataType);
    }

    private static ColumnDeclarationEx columnFromArray(RecordClassDescriptor descriptor, DataFieldInfo dataField, ClassDescriptor[] descriptors) {
        ArrayDataType arrayDataType = (ArrayDataType) dataField.getType();
        com.epam.deltix.qsrv.hf.pub.md.DataType tbDataType = arrayDataType.getElementDataType();

        if (tbDataType instanceof ArrayDataType) {
            throw new UnsupportedOperationException();
        } else if (tbDataType instanceof BinaryDataType) {
            throw new UnsupportedOperationException();
        } else if (tbDataType instanceof ClassDataType) {
            ClassDataType classDataType = (ClassDataType) tbDataType;
            RecordClassDescriptor[] descriptors1 = classDataType.getDescriptors();

            List<ColumnDeclarationEx> columns = new ArrayList<>();
            columns.add(new ColumnDeclarationEx(TYPE_COLUMN_NAME, new NullableDataType(new StringDataType()), false, true));
            Set<String> columnNames = new HashSet<>();
            List<ColumnDeclarationEx> dataColumns = new ArrayList<>();
            for (RecordClassDescriptor classDescriptor : descriptors1) {
                RecordLayout recordLayout = new RecordLayout(classDescriptor);

                for (DataFieldInfo df : recordLayout.getNonStaticFields()) {
                    ColumnDeclarationEx columnDeclarationStream = timebaseDataFieldToClickhouseColumnDeclaration(classDescriptor, df, descriptors/*, dataField.getName()*/);
                    dataColumns.add(columnDeclarationStream);
                }

                for (ColumnDeclarationEx column : dataColumns) {
                    if (columnNames.contains(column.getDbColumnName()))
                        continue;
                    columns.add(column);
                    columnNames.add(column.getDbColumnName());
                }
            }
            NestedDataType dataType = new NestedDataType(columns.stream().map(column -> (ColumnDeclaration) column).collect(Collectors.toList()));

            return new ColumnDeclarationEx(getColumnName(descriptor, dataField, dataType),
                    dataType);

        } else if (tbDataType instanceof BooleanDataType || tbDataType instanceof CharDataType || tbDataType instanceof DateTimeDataType
                || tbDataType instanceof EnumDataType || tbDataType instanceof FloatDataType || tbDataType instanceof IntegerDataType
                || tbDataType instanceof TimeOfDayDataType || tbDataType instanceof VarcharDataType) {
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", tbDataType.getClass().getName()));
        }

        return new ColumnDeclarationEx(getColumnName(descriptor, dataField),
                new deltix.clickhouse.schema.types.ArrayDataType(convertTimebaseDataTypeToClickhouseDataType(tbDataType)));
    }

    public static String getColumnName(RecordClassDescriptor descriptor, DataFieldInfo dataField) {
        com.epam.deltix.qsrv.hf.pub.md.DataType dbDataType = dataField.getType();
        DataType clickhouseDataType = convertTimebaseDataTypeToClickhouseDataType(dbDataType);

        return getColumnName(dbDataType, clickhouseDataType, dataField.getName());
    }

    public static String getColumnName(RecordClassDescriptor descriptor, DataFieldInfo dataField, DataType clickhouseDataType) {
        com.epam.deltix.qsrv.hf.pub.md.DataType dbDataType = dataField.getType();

        return getColumnName(dbDataType, clickhouseDataType, dataField.getName());
    }

    private static String getColumnName(com.epam.deltix.qsrv.hf.pub.md.DataType dbDataType, DataType clickhouseDataType, String name) {
        return encodeColumnName(name + formatTypePostfix(clickhouseDataType, dbDataType));
    }

    private static String formatTypePostfix(DataType clickhouseDataType, com.epam.deltix.qsrv.hf.pub.md.DataType dbDataType) {
        if (clickhouseDataType instanceof NullableDataType)
            return "_N" + formatTypePostfix(((NullableDataType) clickhouseDataType).getNestedType(), dbDataType);

        if (clickhouseDataType instanceof Enum8DataType || clickhouseDataType instanceof Enum16DataType)
            return "_"+ getEnumDbName(dbDataType);

        if (clickhouseDataType instanceof NestedDataType)
            return "";

        if (clickhouseDataType instanceof ObjectDataType)
            return "";

        if (dbDataType instanceof ClassDataType)
            return "";


        return "_" + clickhouseDataType.toString();

    }

    private static String getEnumDbName(com.epam.deltix.qsrv.hf.pub.md.DataType dbDataType) {
        final String typeName = dbDataType.getBaseName();
        final int lastDot = typeName.lastIndexOf(".");
        return lastDot == -1 ? typeName : typeName.substring(lastDot+1);
    }

    private static String encodeColumnName(String name) {
        final String firstLetterValidPattern = "[a-zA-Z_]";
        final String notFirstLetterValidSymbols = "[0-9a-zA-Z_]";
        final Function<Character, String> firstSymbolReplacement = (character) -> String.format("%s%s", COLUMN_NAME_PART_SEPARATOR, character);
        final Function<Character, String> otherSymbolReplacement = (character) -> COLUMN_NAME_PART_SEPARATOR;

        StringBuilder first = StringUtil.replaceInvalidCharacters(name, firstLetterValidPattern, firstSymbolReplacement, 0, 1);
        StringBuilder other = StringUtil.replaceInvalidCharacters(name, notFirstLetterValidSymbols, otherSymbolReplacement, 1, name.length());

        if (other.length() > 1) {
            char[] last = new char[1];
            other.getChars(other.length() - 1, other.length(), last, 0);
            if (last[0] == '_')
                other.setLength(other.length() - 1);
        }
        return first.append(other).toString();
    }

    private static DataType convertTimebaseDataTypeToClickhouseDataType(com.epam.deltix.qsrv.hf.pub.md.DataType tbDataType) {
        DataType essentialDataType;

        if (tbDataType instanceof ArrayDataType) {
            essentialDataType = new deltix.clickhouse.schema.types.ArrayDataType(convertTimebaseDataTypeToClickhouseDataType(((ArrayDataType) tbDataType).getElementDataType()));
        } else if (tbDataType instanceof BinaryDataType) {
            essentialDataType = new StringDataType();
            // @PH: can't use FixedStringDataType, because can't provide MaxSize for BinaryDataType.
            // Introspector don't parse 'maximum' paramater of SchemaType annotation for BinaryDataType.
/*            BinaryDataType binaryDataType = (BinaryDataType) tbDataType;
            essentialDataType = new FixedStringDataType(binaryDataType.getMaxSize());*/
        } else if (tbDataType instanceof BooleanDataType) {
            essentialDataType = new UInt8DataType();
        } else if (tbDataType instanceof CharDataType) {
            essentialDataType = new StringDataType();
            // @PH: in this case, write data to CH as bytes
            //essentialDataType = new FixedStringDataType(2);
        } else if (tbDataType instanceof ClassDataType) {
            essentialDataType = new NothingDataType();
        } else if (tbDataType instanceof DateTimeDataType) {
            essentialDataType = new DateTime64DataType();
        } else if (tbDataType instanceof EnumDataType) {
            List<Enum16DataType.Enum16Value> enumValues = Arrays.stream(((EnumDataType) tbDataType).descriptor.getValues())
                    .map(item -> new Enum16DataType.Enum16Value(item.symbol, (short) item.value))
                    .collect(Collectors.toList());
            essentialDataType = new Enum16DataType(enumValues);
        } else if (tbDataType instanceof FloatDataType) {
            FloatDataType tbFloatDataType = (FloatDataType) tbDataType;

            if (tbFloatDataType.getScale() == FloatDataType.FIXED_FLOAT) // or encoding?
                essentialDataType = new Float32DataType();
            else if (tbFloatDataType.getScale() == FloatDataType.FIXED_DOUBLE ||
                    tbFloatDataType.getScale() == FloatDataType.SCALE_AUTO)
                essentialDataType = new Float64DataType();
            else if (tbFloatDataType.getScale() == FloatDataType.SCALE_DECIMAL64)
                essentialDataType = new Decimal128DataType(DEFAULT_DECIMAL_SCALE);
            else
                essentialDataType = new Float64DataType();
//                throw new UnsupportedOperationException(String.format("Unexpected scale %d for FloatDataType", tbFloatDataType.getScale()));
        } else if (tbDataType instanceof IntegerDataType) {
            IntegerDataType tbIntegerDataType = (IntegerDataType) tbDataType;

            if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT8))
                essentialDataType = new Int8DataType();
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT16))
                essentialDataType = new Int16DataType();
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT32))
                essentialDataType = new Int32DataType();
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT64))
                essentialDataType = new Int64DataType();
            else
                throw new UnsupportedOperationException(String.format("Unexpected encoding %s for IntegerDataType", tbIntegerDataType.getEncoding()));
        } else if (tbDataType instanceof TimeOfDayDataType) {
            essentialDataType = new Int32DataType();
        } else if (tbDataType instanceof VarcharDataType) {
            essentialDataType = new StringDataType();
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", tbDataType.getClass().getName()));
        }

        return tbDataType.isNullable() ?
                new NullableDataType(essentialDataType) : essentialDataType;
    }

    public static ClickHouseDataType convertDataTypeToRawClickhouseDataType(DataType dataType) {
        ClickHouseDataType essentialDataType;

        if (dataType instanceof NullableDataType)
            dataType = ((NullableDataType) dataType).getNestedType();

        if (dataType instanceof deltix.clickhouse.schema.types.ArrayDataType) {
            essentialDataType = ClickHouseDataType.Array;
//        } else if (dataType instanceof BinaryDataType) {
//            essentialDataType = ClickHouseDataType.String;
        } else if (dataType instanceof UInt8DataType) {
            essentialDataType = ClickHouseDataType.UInt8;
        } else if (dataType instanceof StringDataType) {
            essentialDataType = ClickHouseDataType.String;
//        } else if (dataType instanceof ClassDataType) {
//            throw new NotImplementedException();
        } else if (dataType instanceof NothingDataType) {
            essentialDataType = ClickHouseDataType.Nothing;
        } else if (dataType instanceof DateDataType) {
            essentialDataType = ClickHouseDataType.Date;
        } else if (dataType instanceof DateTime64DataType) {
            essentialDataType = ClickHouseDataType.DateTime;
        } else if (dataType instanceof Enum16DataType) {
            essentialDataType = ClickHouseDataType.Enum16;
        } else if (dataType instanceof Float32DataType) {
            essentialDataType = ClickHouseDataType.Float32;
        } else if (dataType instanceof Float64DataType) {
            essentialDataType = ClickHouseDataType.Float64;
        } else if (dataType instanceof Decimal128DataType) {
            essentialDataType = ClickHouseDataType.Decimal128;
        } else if (dataType instanceof Int8DataType) {
            essentialDataType = ClickHouseDataType.Int8;
        } else if (dataType instanceof Int16DataType) {
            essentialDataType = ClickHouseDataType.Int16;
        } else if (dataType instanceof Int32DataType) {
            essentialDataType = ClickHouseDataType.Int32;
        } else if (dataType instanceof Int64DataType) {
            essentialDataType = ClickHouseDataType.Int64;
        } else if (dataType instanceof NestedDataType) {
            essentialDataType = ClickHouseDataType.Nested;
        } else if (dataType instanceof ObjectDataType) {
            essentialDataType = ClickHouseDataType.Nothing;
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", dataType.getClass().getName()));
        }
        return essentialDataType;
    }

    public static Object convertDataTypeToDefaultValue(DataType dataType) {
        Object essentialDataType;

        if (dataType instanceof NullableDataType) {
            essentialDataType = null;
        } else if (dataType instanceof deltix.clickhouse.schema.types.ArrayDataType) {
            essentialDataType = ClickHouseDataType.Array;
//        } else if (dataType instanceof BinaryDataType) {
//            essentialDataType = ClickHouseDataType.String;
        } else if (dataType instanceof UInt8DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof StringDataType) {
            essentialDataType = "";
//        } else if (dataType instanceof ClassDataType) {
//            throw new NotImplementedException();
        } else if (dataType instanceof DateDataType) {
            essentialDataType = new Date(0);
        } else if (dataType instanceof DateTime64DataType) {
            essentialDataType = new Timestamp(0);
        } else if (dataType instanceof Enum16DataType) {
            essentialDataType = ((Enum16DataType) dataType).getValues().get(0).getName();
        } else if (dataType instanceof Float32DataType) {
            essentialDataType = 0.0;
        } else if (dataType instanceof Float64DataType) {
            essentialDataType = 0.0;
        } else if (dataType instanceof Decimal128DataType) {
            essentialDataType = Decimal64Utils.ZERO;
        } else if (dataType instanceof Int8DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof Int16DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof Int32DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof Int64DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof NestedDataType) {
            essentialDataType = null;
        } else if (dataType instanceof ObjectDataType) {
            essentialDataType = null;
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", dataType.getClass().getName()));
        }
        return essentialDataType;
    }

    public static ClickHouseDataType convertTimebaseDataTypeToRawClickhouseDataType(com.epam.deltix.qsrv.hf.pub.md.DataType tbDataType) {
        ClickHouseDataType essentialDataType;

        if (tbDataType instanceof ArrayDataType) {
            essentialDataType = ClickHouseDataType.Array;
        } else if (tbDataType instanceof BinaryDataType) {
            essentialDataType = ClickHouseDataType.String;
        } else if (tbDataType instanceof BooleanDataType) {
            essentialDataType = ClickHouseDataType.UInt8;
        } else if (tbDataType instanceof CharDataType) {
            essentialDataType = ClickHouseDataType.String;
        } else if (tbDataType instanceof ClassDataType) {
            essentialDataType = ClickHouseDataType.Nothing;;
        } else if (tbDataType instanceof DateTimeDataType) {
            essentialDataType = ClickHouseDataType.DateTime;
        } else if (tbDataType instanceof EnumDataType) {
            essentialDataType = ClickHouseDataType.Enum16;
        } else if (tbDataType instanceof FloatDataType) {
            FloatDataType tbFloatDataType = (FloatDataType) tbDataType;

            if (tbFloatDataType.getScale() == FloatDataType.FIXED_FLOAT) // or encoding?
                essentialDataType = ClickHouseDataType.Float32;
            else if (tbFloatDataType.getScale() == FloatDataType.FIXED_DOUBLE)
                essentialDataType = ClickHouseDataType.Float64;
            else if (tbFloatDataType.getScale() == FloatDataType.SCALE_AUTO ||
                    tbFloatDataType.getScale() == FloatDataType.SCALE_DECIMAL64)
                essentialDataType = ClickHouseDataType.Decimal128;
            else
                essentialDataType = ClickHouseDataType.Float64;
        } else if (tbDataType instanceof IntegerDataType) {
            IntegerDataType tbIntegerDataType = (IntegerDataType) tbDataType;

            if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT8))
                essentialDataType = ClickHouseDataType.Int8;
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT16))
                essentialDataType = ClickHouseDataType.Int16;
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT32))
                essentialDataType = ClickHouseDataType.Int32;
            else if (tbIntegerDataType.getEncoding().equals(IntegerDataType.ENCODING_INT64))
                essentialDataType = ClickHouseDataType.Int64;
            else
                throw new UnsupportedOperationException(String.format("Unexpected encoding %s for IntegerDataType", tbIntegerDataType.getEncoding()));
        } else if (tbDataType instanceof TimeOfDayDataType) {
            essentialDataType = ClickHouseDataType.Int32;
        } else if (tbDataType instanceof VarcharDataType) {
            essentialDataType = ClickHouseDataType.String;
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", tbDataType.getClass().getName()));
        }

//        return tbDataType.isNullable() ?
//                new NullableDataType(essentialDataType) : essentialDataType;
        return essentialDataType;
    }

    public Map<RecordClassDescriptor, List<ColumnDeclarationEx>> getColumnDeclarations() {
        return columnDeclarations;
    }

    public List<ColumnDeclarationEx> getAllColumns() {
        return allColumns;
    }

    public TableDeclaration prepareClickhouseTable() throws SQLException {
        return adjustTargetSchema();
    }

    private TableDeclaration adjustTargetSchema() throws SQLException {
        TableDeclaration clickhouseTableDeclaration = timebaseStreamToClickhouseTable(timebaseStream, columnDeclarations, allColumns, clickhouseProperties.getDatabase());

        boolean createIfNotExists = true;

        LOG.debug()
                .append("Creating database ")
                .append(clickhouseTableDeclaration.getTableIdentity().getDatabaseName())
                .append(" if not exists = ")
                .append(createIfNotExists)
                .commit();

        clickhouseClient.createDatabase(clickhouseTableDeclaration.getTableIdentity().getDatabaseName(), createIfNotExists);

        clickhouseClient.createTable(clickhouseTableDeclaration,
                new MergeTreeEngine(clickhouseTableDeclaration.getColumns().get(0),
                        Stream.of(clickhouseTableDeclaration.getColumns().get(1), clickhouseTableDeclaration.getColumns().get(2)).collect(Collectors.toList())), createIfNotExists);

        return clickhouseTableDeclaration;
    }


}
