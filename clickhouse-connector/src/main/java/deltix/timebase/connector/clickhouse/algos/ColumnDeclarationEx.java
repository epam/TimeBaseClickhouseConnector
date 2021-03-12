package deltix.timebase.connector.clickhouse.algos;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.types.DataType;
import deltix.clickhouse.schema.types.NestedDataType;
import org.apache.commons.lang3.tuple.Pair;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ColumnDeclarationEx extends ColumnDeclaration {
    public static String nestedClassDelimiter = "_";

    private final ClickHouseDataType clickHouseDataType;
    private final Object defaultValue;
    public Consumer<UnboundTableWriter.ClickhouseContext> writeNull;
    private int statementIndex;

    public ColumnDeclarationEx(String name, deltix.clickhouse.schema.types.DataType dataType) {
        super(name, dataType);
        this.clickHouseDataType = SchemaProcessor.convertDataTypeToRawClickhouseDataType(dataType);
        this.defaultValue = SchemaProcessor.convertDataTypeToDefaultValue(dataType);
        writeNull = UnboundTableWriter.buildWriteNull(this);
    }

    public ColumnDeclarationEx(String name, deltix.clickhouse.schema.types.DataType dataType, boolean partition, boolean index) {
        super(name, dataType, partition, index);
        this.clickHouseDataType = SchemaProcessor.convertDataTypeToRawClickhouseDataType(dataType);
        this.defaultValue = SchemaProcessor.convertDataTypeToDefaultValue(dataType);
        writeNull = UnboundTableWriter.buildWriteNull(this);
    }

    public static List<Pair<String, String>> getColumnSqlDefinitions(ColumnDeclaration column) {
        final DataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<Pair<String, String>> result = new ArrayList<Pair<String, String>>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
//                final List<Pair<String, String>> pairs = getColumnSqlDefinitions(c);
                for (Pair<String, String> pair : getColumnSqlDefinitions(c))
                    result.add(Pair.of(column.getDbColumnName() + nestedClassDelimiter + pair.getLeft(), pair.getRight()));
            }
            return result;
            //return objectDataType.getColumns().stream().flatMap(column1 -> getColumnSqlDefinitions(column1).stream()).collect(Collectors.toList());
        } else
            return List.of(Pair.of(column.getDbColumnName(), column.getDbDataType().getSqlDefinition()));
    }

    public static List<ColumnDeclarationEx> getColumnsDeep(ColumnDeclarationEx column) {
        final DataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
                for (ColumnDeclarationEx c1 : getColumnsDeep((ColumnDeclarationEx) c))
                    //result.add(new ColumnDeclarationEx(column.getDbColumnName() + nestedClassDelimiter + c1.getDbColumnName(), c1.getDbDataType()));
                    result.add(c1);
            }
            return result;
        } else if (dataType instanceof NestedDataType) {
            NestedDataType objectDataType = (NestedDataType) dataType;
            List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
                for (ColumnDeclarationEx c1 : getColumnsDeep((ColumnDeclarationEx) c))
                    //result.add(new ColumnDeclarationEx(column.getDbColumnName() + nestedClassDelimiter + c1.getDbColumnName(), c1.getDbDataType()));
                    result.add(c1);
            }
            return result;
        } else
            return List.of(column);
    }

    public static List<ColumnDeclarationEx> getColumnsDeep(List<ColumnDeclarationEx> columns) {
        List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
        for (ColumnDeclarationEx c : columns) {
            for (ColumnDeclarationEx c1 : getColumnsDeep((ColumnDeclarationEx) c))
                result.add(c1);
        }
        return result;
    }

    public static List<ColumnDeclarationEx> getColumnsDeepForDefinition(ColumnDeclarationEx column) {
        final DataType dataType = column.getDbDataType();
        if (dataType instanceof ObjectDataType) {
            ObjectDataType objectDataType = (ObjectDataType) dataType;
            List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
            for (ColumnDeclaration c : objectDataType.getColumns()) {
                for (ColumnDeclarationEx c1 : getColumnsDeepForDefinition((ColumnDeclarationEx) c)) {
                    final ColumnDeclarationEx newColumn = new ColumnDeclarationEx(column.getDbColumnName() + nestedClassDelimiter + c1.getDbColumnName(), c1.getDbDataType());
                    newColumn.setStatementIndex(c1.getStatementIndex());
                    result.add(newColumn);
                }
            }
            return result;
        } else
            return List.of(column);
    }

    public static List<ColumnDeclarationEx> getColumnsDeepForDefinition(List<ColumnDeclarationEx> columns) {
        List<ColumnDeclarationEx> result = new ArrayList<ColumnDeclarationEx>();
        for (ColumnDeclarationEx c : columns) {
            for (ColumnDeclarationEx c1 : getColumnsDeepForDefinition((ColumnDeclarationEx) c))
                result.add(c1);
        }
        return result;
    }

    public ColumnDeclarationEx deepCopy() {
        DataType dataType = getDbDataType();

        if (dataType instanceof ObjectDataType) {
            final ObjectDataType objectDataType = (ObjectDataType) dataType;
            final List<ColumnDeclaration> columns = objectDataType.getColumns().stream().map(c -> ((ColumnDeclarationEx) c).deepCopy()).collect(Collectors.toList());
            dataType = new ObjectDataType(objectDataType.getColumnName(), columns);
        } else if (dataType instanceof NestedDataType) {
            final NestedDataType nestedDataType = (NestedDataType) dataType;
            final List<ColumnDeclaration> columns = nestedDataType.getColumns().stream().map(c -> ((ColumnDeclarationEx) c).deepCopy()).collect(Collectors.toList());
            dataType = new NestedDataType(columns);
        }

        return new ColumnDeclarationEx(getDbColumnName(), dataType, isPartition(), isIndex());
    }

    public int getStatementIndex() {
        return statementIndex;
    }

    public void setStatementIndex(int statementIndex) {
        this.statementIndex = statementIndex;
    }

    @Override
    public String getSqlDefinition() {
        final List<ColumnDeclarationEx> columns = getColumnsDeepForDefinition(this);
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            final ColumnDeclarationEx columnDeclarationEx = columns.get(i);
            sb.append(String.format("`%s` %s", columnDeclarationEx.getDbColumnName(), columnDeclarationEx.getDbDataType()));
        }

        return sb.toString();
    }

    public ClickHouseDataType getClickHouseDataType() {
        return clickHouseDataType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "{ si=" + statementIndex +
                super.toString() +
                '}';
    }
}
