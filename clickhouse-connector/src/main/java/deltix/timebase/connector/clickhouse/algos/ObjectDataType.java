package deltix.timebase.connector.clickhouse.algos;

import deltix.clickhouse.schema.ColumnDeclaration;
import deltix.clickhouse.schema.types.BaseDataType;
import deltix.clickhouse.schema.types.DataTypes;

import java.util.Collections;
import java.util.List;

public class ObjectDataType extends BaseDataType {

    private final String columnName;
    private final List<ColumnDeclaration> columns;

    public ObjectDataType(String columnName, List<ColumnDeclaration> columns) {
        super(DataTypes.NOTHING);
        this.columnName = columnName;

        this.columns = Collections.unmodifiableList(columns);
    }

    public List<ColumnDeclaration> getColumns() {
        return columns;
    }

    @Override
    public String getSqlDefinition() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < columns.size(); ++i) {
            if (i > 0)
                sb.append(", ");

            sb.append(String.format("`%s.%s` %s", columnName, columns.get(i).getDbColumnName(), columns.get(i).getDbDataType()));
        }

        return sb.toString();
//        final String result = getSqlDefinitionForComplexType(columns);
//        return result;

    }

    public String getColumnName() {
        return columnName;
    }
}
