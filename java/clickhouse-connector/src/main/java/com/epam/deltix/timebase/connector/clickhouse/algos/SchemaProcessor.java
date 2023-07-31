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

import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.value.ClickHouseDateValue;
import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.qsrv.hf.pub.md.*;
import com.epam.deltix.qsrv.hf.pub.md.DateTimeDataType;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.models.ClickhouseTableIdentity;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.engines.Engine;
import com.epam.deltix.clickhouse.schema.engines.MergeTreeEngine;
import com.epam.deltix.clickhouse.util.SqlQueryHelper;
import com.epam.deltix.dfp.Decimal64Utils;
import com.epam.deltix.gflog.api.Log;
import com.epam.deltix.gflog.api.LogFactory;
import com.epam.deltix.qsrv.hf.pub.codec.DataFieldInfo;
import com.epam.deltix.qsrv.hf.pub.codec.NonStaticFieldLayout;
import com.epam.deltix.qsrv.hf.pub.codec.RecordLayout;
import com.epam.deltix.timebase.connector.clickhouse.configuration.properties.ClickhouseProperties;
import com.epam.deltix.timebase.connector.clickhouse.model.ColumnNamingScheme;
import com.epam.deltix.timebase.connector.clickhouse.model.SchemaOptions;
import com.epam.deltix.timebase.connector.clickhouse.model.WriteMode;
import com.epam.deltix.timebase.connector.clickhouse.util.StringUtil;
import com.epam.deltix.util.collections.DuplicateKeyException;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.epam.deltix.timebase.connector.clickhouse.algos.QueryReplicator.ALL_TYPES;
import static com.epam.deltix.util.lang.Util.getSimpleName;

public class SchemaProcessor {

    public static final String PARTITION_COLUMN_NAME = "partition";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String INSTRUMENT_COLUMN_NAME = "instrument";
    public static final String TYPE_COLUMN_NAME = "type";
    public static final Object[] EMPTY_ARRAY = new Object[0];

    public static final String[] FIXED_COLUMN_NAMES = {PARTITION_COLUMN_NAME, TIMESTAMP_COLUMN_NAME, INSTRUMENT_COLUMN_NAME, TYPE_COLUMN_NAME};

    public static final String COLUMN_NAME_PART_SEPARATOR = "_";
    public static final int DEFAULT_DECIMAL_SCALE = 12;
    public static final int DEFAULT_DECIMAL_PRECISION = 38;

    protected static final Log LOG = LogFactory.getLog(SchemaProcessor.class);
    public static final NestedDataType MOCK_NESTED_DATA_TYPE = new NestedDataType(new ArrayList<>());
    private final SchemaOptions schemaOptions;
    private final ClickhouseClient clickhouseClient;
    private final ClickhouseProperties clickhouseProperties;

    private final Map<String, List<ColumnDeclarationEx>> columnDeclarations = new HashMap<>();

    public SchemaProcessor(SchemaOptions schemaOptions,
                           ClickhouseClient clickhouseClient,
                           ClickhouseProperties clickhouseProperties) {
        this.schemaOptions = schemaOptions;
        this.clickhouseClient = clickhouseClient;
        this.clickhouseProperties = clickhouseProperties;
    }

    public Map<String, TableDeclaration> timebaseStreamToClickhouseTable() {
        RecordClassSet tbSchema = schemaOptions.getTbSchema();
        Map<String, String> mapping = schemaOptions.getMapping();

        RecordClassDescriptor[] allDescriptors = Arrays.stream(tbSchema.getClassDescriptors())
                .filter(rcd -> rcd instanceof RecordClassDescriptor && !((RecordClassDescriptor)rcd).isAbstract())
                .map(classDescriptor -> (RecordClassDescriptor) classDescriptor)
                .toArray(RecordClassDescriptor[]::new);

        for (RecordClassDescriptor descriptor : allDescriptors) {
            columnDeclarations.put(descriptor.getName(), getColumnDeclarations(descriptor));
        }
        Map<String, TableDeclaration> tableDeclarations = new HashMap<>(mapping.size());

        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            ClickhouseTableIdentity tableIdentity = ClickhouseTableIdentity.of(clickhouseProperties.getDatabase(), normalizeStreamNameToClickhouseNotation(entry.getValue()));
            if (entry.getKey().equals(ALL_TYPES)) {
                List<ColumnDeclaration> columnDeclarations = getColumnDeclarations(tbSchema.getContentClasses())
                        .stream()
                        .map(c -> (ColumnDeclaration)c)
                        .collect(Collectors.toList());
                tableDeclarations.put(entry.getKey(), new TableDeclaration(tableIdentity, columnDeclarations));
            } else if (columnDeclarations.containsKey(entry.getKey())){
                List<ColumnDeclaration> columnDeclarations = this.columnDeclarations.get(entry.getKey())
                        .stream()
                        .map(c -> (ColumnDeclaration)c)
                        .collect(Collectors.toList());
                tableDeclarations.put(entry.getKey(), new TableDeclaration(tableIdentity, columnDeclarations));
            } else {
                throw new IllegalArgumentException("Invalidate mapping");
            }
        }

        return tableDeclarations;
    }

    private List<ColumnDeclarationEx> getColumnDeclarations(RecordClassDescriptor...rcds) {

        Map<String, ColumnDeclarationEx> allColumnNames = new HashMap<>();
        List<ColumnDeclarationEx> columnDeclarations = new ArrayList<>();

        if (schemaOptions.isIncludePartitionColumn()) {
            ColumnDeclarationEx partition = new ColumnDeclarationEx(PARTITION_COLUMN_NAME,
                    new DateDataType(), true, false);
            columnDeclarations.add(partition);
        }

        ColumnDeclarationEx timestamp = new ColumnDeclarationEx(TIMESTAMP_COLUMN_NAME,
                new DateTime64DataType(9), false, false);
        columnDeclarations.add(timestamp);

        ColumnDeclarationEx instrument = new ColumnDeclarationEx(INSTRUMENT_COLUMN_NAME,
                new StringDataType(), false, true);
        columnDeclarations.add(instrument);
        columnDeclarations.add(new ColumnDeclarationEx(TYPE_COLUMN_NAME, new StringDataType(), false, true));

        for (RecordClassDescriptor descriptor : rcds) {
            RecordLayout recordLayout = new RecordLayout(descriptor);

            List<ColumnDeclarationEx> dataColumns = new ArrayList<>();
            final NonStaticFieldLayout[] nonStaticFields = recordLayout.getNonStaticFields();
            if (nonStaticFields == null)
                continue;

            for (NonStaticFieldLayout dataField : nonStaticFields) {
                ColumnDeclarationEx columnDeclarationStream = getColumnDeclaration(descriptor, dataField);
                dataColumns.add(columnDeclarationStream);
            }
            for (int i = 0; i < dataColumns.size(); i++) {
                ColumnDeclarationEx column = dataColumns.get(i);
                ColumnDeclarationEx columnProcessed = allColumnNames.get(column.getDbColumnName());
                if (columnProcessed != null) {
                    if (!columnProcessed.getDbDataType().getSqlDefinition().equals(column.getDbDataType().getSqlDefinition())) {
                        throw new DuplicateKeyException(String.format("Type: %s generates multiple columns with the same name (%s) but different types. " +
                                "Try using splitByTypes or change columnNamingScheme", descriptor.getName(), column.getDbColumnName()));
                    }
                } else {
                    columnDeclarations.add(column);
                    allColumnNames.put(column.getDbColumnName(), column);
                }
            }
        }
        return columnDeclarations;
    }

    public static String normalizeStreamNameToClickhouseNotation(String streamName) {
        if (streamName == null)
            throw new IllegalArgumentException("Stream name can't be null");

        return encodeColumnName(streamName);
    }

    public ColumnDeclarationEx getColumnDeclaration(RecordClassDescriptor descriptor, DataFieldInfo dataField) {
        DataType tbDataType = dataField.getType();
        if (tbDataType instanceof com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) {
            return columnFromArray(descriptor, dataField);
        } else if (tbDataType instanceof ClassDataType) {
            return columnFromClass(descriptor, dataField);
        }

        SqlDataType dataType = convertTimebaseDataTypeToClickhouseDataType(tbDataType);
//        if (isArray)
//            dataType = new deltix.clickhouse.schema.types.ArrayDataType(dataType);
        return new ColumnDeclarationEx(getColumnName(descriptor, dataField), dataType);
    }

    private ColumnDeclarationEx columnFromClass(RecordClassDescriptor descriptor, DataFieldInfo dataField) {
        ClassDataType classDataType = (ClassDataType) dataField.getType();
        RecordClassDescriptor[] descriptors = classDataType.getDescriptors();

        List<ColumnDeclaration> columns = getColumnDeclarationsFromClass(descriptors);

        final String columnName = getColumnName(descriptor, dataField);
        ObjectDataType dataType = new ObjectDataType(columnName, columns);
        return new ColumnDeclarationEx(columnName, dataType);
    }

    private ColumnDeclarationEx columnFromArray(RecordClassDescriptor descriptor, DataFieldInfo dataField) {
        com.epam.deltix.qsrv.hf.pub.md.ArrayDataType arrayDataType = (com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) dataField.getType();
        DataType tbDataType = arrayDataType.getElementDataType();

        String columnName = getColumnName(descriptor, dataField);
        if (tbDataType instanceof com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) {
            throw new UnsupportedOperationException();
        } else if (tbDataType instanceof BinaryDataType) {
            throw new UnsupportedOperationException();
        } else if (tbDataType instanceof ClassDataType) {
            ClassDataType classDataType = (ClassDataType) tbDataType;
            RecordClassDescriptor[] descriptors = classDataType.getDescriptors();

            List<ColumnDeclaration> columns = getColumnDeclarationsFromClass(descriptors);

            NestedDataType dataType = new NestedDataType(columns);
            return new ColumnDeclarationEx(columnName, dataType);

        } else if (tbDataType instanceof BooleanDataType || tbDataType instanceof CharDataType || tbDataType instanceof DateTimeDataType
                || tbDataType instanceof EnumDataType || tbDataType instanceof FloatDataType || tbDataType instanceof IntegerDataType
                || tbDataType instanceof TimeOfDayDataType || tbDataType instanceof VarcharDataType) {
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", tbDataType.getClass().getName()));
        }

        return new ColumnDeclarationEx(columnName,
                new ArraySqlType(convertTimebaseDataTypeToClickhouseDataType(tbDataType)));
    }

    private List<ColumnDeclaration> getColumnDeclarationsFromClass(RecordClassDescriptor[] descriptors) {
        Map<String, ColumnDeclarationEx> columns = new LinkedHashMap<>();

        columns.put(TYPE_COLUMN_NAME, new ColumnDeclarationEx(TYPE_COLUMN_NAME, new NullableDataType(new StringDataType()), false, true));

        for (RecordClassDescriptor classDescriptor : descriptors) {
            RecordLayout recordLayout = new RecordLayout(classDescriptor);

            for (DataFieldInfo df : recordLayout.getNonStaticFields()) {
                ColumnDeclarationEx column = getColumnDeclaration(classDescriptor, df);

                if (columns.containsKey(column.getDbColumnName())) {
                    if (!columns.get(column.getDbColumnName()).getSqlDefinition().equals(column.getSqlDefinition())) {
                        throw new DuplicateKeyException(String.format("Type: %s generates multiple columns with the same name (%s) but different types. " +
                                "Try using splitByTypes or change columnNamingScheme", classDescriptor.getName(), column.getDbColumnName()));
                    }
                } else {
                    columns.put(column.getDbColumnName(), column);
                }
            }
        }
        return columns.values().stream().map(column -> (ColumnDeclaration) column).collect(Collectors.toList());
    }

    public String getColumnName(RecordClassDescriptor descriptor, DataFieldInfo dataField) {
        return getColumnName(descriptor, dataField, schemaOptions.getColumnNamingScheme());
    }

    public static String getColumnName(RecordClassDescriptor descriptor, DataFieldInfo dataField, ColumnNamingScheme pattern) {
        String columnName;
        switch (pattern) {
            case TYPE_AND_NAME:
                columnName = getSimpleName(descriptor.getName()) + COLUMN_NAME_PART_SEPARATOR + dataField.getName();
                break;
            case NAME:
                columnName = dataField.getName();
                break;
            case NAME_AND_DATATYPE:
                if (isNestedType(dataField)) {
                    columnName = getColumnNameWithDataType(dataField.getType(), MOCK_NESTED_DATA_TYPE, dataField.getName());
                } else {
                    columnName = getColumnNameWithDataType(dataField);
                }
                break;
            default:
                throw new IllegalArgumentException("Illegal column name pattern: " + pattern);
        }
        return encodeColumnName(columnName);
    }

    private static boolean isNestedType(DataFieldInfo dataField) {
        if (dataField.getType() instanceof ArrayDataType) {
            return ((ArrayDataType) dataField.getType()).getElementDataType() instanceof ClassDataType;
        }
        return false;
    }

    private static String getColumnNameWithDataType(DataFieldInfo dataField) {
        DataType dbDataType = dataField.getType();
        SqlDataType clickhouseDataType = convertTimebaseDataTypeToClickhouseDataType(dbDataType);

        return getColumnNameWithDataType(dbDataType, clickhouseDataType, dataField.getName());
    }

    public static String getColumnNameWithDataType(DataType dbDataType, SqlDataType clickhouseDataType, String name) {
        return encodeColumnName(name + formatTypePostfix(clickhouseDataType, dbDataType));
    }

    private static String formatTypePostfix(SqlDataType clickhouseDataType, DataType dbDataType) {
        if (clickhouseDataType instanceof NullableDataType)
            return "_N" + formatTypePostfix(((NullableDataType) clickhouseDataType).getNestedType(), dbDataType);

        if (clickhouseDataType instanceof Enum8DataType || clickhouseDataType instanceof Enum16DataType)
            return "_" + getEnumDbName(dbDataType);

        if (clickhouseDataType instanceof NestedDataType)
            return "";

        if (clickhouseDataType instanceof ObjectDataType)
            return "";

        if (dbDataType instanceof ClassDataType)
            return "";

        if (clickhouseDataType instanceof DecimalDataType) {
            DecimalDataType decimalDataType = (DecimalDataType) clickhouseDataType;
            if (decimalDataType.getP() == 38)
                return "_" + new Decimal128DataType(decimalDataType.getS()).toString();
        }

        return "_" + clickhouseDataType.toString();

    }

    private static String getEnumDbName(DataType dbDataType) {
        final String typeName = dbDataType.getBaseName();
        final int lastDot = typeName.lastIndexOf(".");
        return lastDot == -1 ? typeName : typeName.substring(lastDot + 1);
    }

    public static String encodeColumnName(String name) {
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

    public static SqlDataType convertTimebaseDataTypeToClickhouseDataType(DataType tbDataType) {
        SqlDataType essentialDataType;

        if (tbDataType instanceof com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) {
            essentialDataType = new ArraySqlType(
                    convertTimebaseDataTypeToClickhouseDataType(((com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) tbDataType).getElementDataType()));
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
                essentialDataType = new DecimalDataType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            else
                essentialDataType = new Float64DataType();
//                throw new UnsupportedOperationException(String.format("Unexpected scale %d for FloatDataType", tbFloatDataType.getScale()));
        } else if (tbDataType instanceof IntegerDataType) {
            IntegerDataType tbIntegerDataType = (IntegerDataType) tbDataType;

            switch (tbIntegerDataType.getNativeTypeSize()) {
                case 1:
                    essentialDataType = new Int8DataType();
                    break;
                case 2:
                    essentialDataType = new Int16DataType();
                    break;
                case 4:
                    essentialDataType = new Int32DataType();
                    break;
                default:
                    essentialDataType = new Int64DataType();
            }
        } else if (tbDataType instanceof TimeOfDayDataType) {
            essentialDataType = new Int32DataType();
        } else if (tbDataType instanceof VarcharDataType) {
            essentialDataType = new StringDataType();
        } else {
            throw new UnsupportedOperationException(String.format("Cannot convert data type '%s'", tbDataType.getClass().getName()));
        }

        return tbDataType.isNullable() && !(tbDataType instanceof com.epam.deltix.qsrv.hf.pub.md.ArrayDataType) ?
                new NullableDataType(essentialDataType) : essentialDataType;
    }

    public static ClickHouseDataType convertDataTypeToRawClickhouseDataType(SqlDataType dataType) {
        ClickHouseDataType essentialDataType;

        if (dataType instanceof NullableDataType)
            dataType = ((NullableDataType) dataType).getNestedType();

        if (dataType instanceof ArraySqlType) {
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
            essentialDataType = ClickHouseDataType.DateTime64;
        } else if (dataType instanceof Enum16DataType) {
            essentialDataType = ClickHouseDataType.Enum16;
        } else if (dataType instanceof Float32DataType) {
            essentialDataType = ClickHouseDataType.Float32;
        } else if (dataType instanceof Float64DataType) {
            essentialDataType = ClickHouseDataType.Float64;
        } else if (dataType instanceof DecimalDataType) {
            essentialDataType = ClickHouseDataType.Decimal;
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

    public static Object convertDataTypeToDefaultValue(SqlDataType dataType) {
        Object essentialDataType;

        if (dataType instanceof NullableDataType) {
            essentialDataType = null;
        } else if (dataType instanceof ArraySqlType) {
            essentialDataType = EMPTY_ARRAY;
//        } else if (dataType instanceof BinaryDataType) {
//            essentialDataType = ClickHouseDataType.String;
        } else if (dataType instanceof UInt8DataType) {
            essentialDataType = 0;
        } else if (dataType instanceof StringDataType) {
            essentialDataType = "";
//        } else if (dataType instanceof ClassDataType) {
//            throw new NotImplementedException();
        } else if (dataType instanceof DateDataType) {
            essentialDataType = ClickHouseDateValue.of(0);
        } else if (dataType instanceof DateTime64DataType) {
            essentialDataType = new Timestamp(0);
        } else if (dataType instanceof Enum16DataType) {
            essentialDataType = ((Enum16DataType) dataType).getValues().get(0).getName();
        } else if (dataType instanceof Float32DataType) {
            essentialDataType = 0.0;
        } else if (dataType instanceof Float64DataType) {
            essentialDataType = 0.0;
        } else if (dataType instanceof DecimalDataType) {
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

    public Map<String, List<ColumnDeclarationEx>> getColumnDeclarations() {
        return columnDeclarations;
    }


    public Map<String, TableDeclaration> prepareClickhouseTable() throws SQLException {
        return adjustTargetSchema();
    }

    private Map<String, TableDeclaration> adjustTargetSchema() throws SQLException {
        Map<String, TableDeclaration> clickhouseTableDeclarations = timebaseStreamToClickhouseTable();

        boolean createIfNotExists = true;

        LOG.debug()
                .append("Creating database ")
                .append(clickhouseProperties.getDatabase())
                .append(" if not exists = ")
                .append(createIfNotExists)
                .commit();

        clickhouseClient.createDatabase(clickhouseProperties.getDatabase(), createIfNotExists);

        for (Map.Entry<String, TableDeclaration> entry : clickhouseTableDeclarations.entrySet()) {
            TableDeclaration tableDeclaration = entry.getValue();
            if (WriteMode.REWRITE == schemaOptions.getWriteMode()) {
                LOG.debug()
                        .append("Drop table if exists ")
                        .append(tableDeclaration.getTableIdentity().toString())
                        .commit();
                clickhouseClient.dropTable(tableDeclaration.getTableIdentity(), true);
            }

            if (clickhouseClient.existsTable(tableDeclaration.getTableIdentity())) {
                TableDeclaration actualTable = clickhouseClient.describeTable(tableDeclaration.getTableIdentity());
                clickhouseTableDeclarations.replace(entry.getKey(), mergeTableDeclaration(tableDeclaration, actualTable));
            } else {
                Engine engine;
                if (schemaOptions.isIncludePartitionColumn()) {
                    engine = new MergeTreeEngine(tableDeclaration.getColumns().get(0), tableDeclaration.getColumns().subList(1, 3));
                } else {
                    engine = new SimpleMergeTreeEngine(tableDeclaration.getColumns().subList(0, 2));
                }
                LOG.debug().append(SqlQueryHelper.getCreateTableQuery(tableDeclaration, engine, createIfNotExists)).commit();
                clickhouseClient.createTable(tableDeclaration, engine, createIfNotExists);
            }

        }
        return clickhouseTableDeclarations;
    }

    private TableDeclaration mergeTableDeclaration(TableDeclaration expectedTable, TableDeclaration actualTable) {
        TableSchemaMerger schemaMerger = new TableSchemaMerger(expectedTable, actualTable);
        if (schemaMerger.mergeSchema()) {
            return new TableDeclaration(actualTable.getTableIdentity(), schemaMerger.getFilterColumns());
        }
        return expectedTable;
    }
}