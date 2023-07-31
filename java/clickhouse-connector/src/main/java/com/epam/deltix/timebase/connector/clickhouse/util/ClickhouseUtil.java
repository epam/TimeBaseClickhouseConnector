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
package com.epam.deltix.timebase.connector.clickhouse.util;

import com.clickhouse.data.ClickHouseDataType;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.schema.TableDeclaration;
import com.epam.deltix.clickhouse.schema.types.Decimal128DataType;
import com.epam.deltix.clickhouse.schema.types.DecimalDataType;
import com.epam.deltix.clickhouse.schema.types.SqlDataType;
import com.epam.deltix.clickhouse.util.SelectQueryHelper;
import com.epam.deltix.timebase.connector.clickhouse.algos.ColumnDeclarationEx;
import com.epam.deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import com.epam.deltix.timebase.connector.clickhouse.algos.TableSchemaMerger;
import com.epam.deltix.util.time.GMT;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ClickhouseUtil {
    public static final BigDecimal DECIMAL_128_MAX_VALUE = getDecimalMaxValue(new Decimal128DataType(SchemaProcessor.DEFAULT_DECIMAL_SCALE));
    public static final BigDecimal DECIMAL_128_MIN_VALUE = getDecimalMinValue(new Decimal128DataType(SchemaProcessor.DEFAULT_DECIMAL_SCALE));

    public static final Long DATETIME_64_MIN_VALUE = -1420070400000L + 1;
    public static final Long DATETIME_64_MAX_VALUE = 9904550399000L - 1;

    public static BigDecimal getDecimalMinValue(SqlDataType dataType) {
        return getDecimalMaxValue(dataType).negate();
    }

    public static BigDecimal getDecimalMaxValue(SqlDataType dataType) {
        final SqlDataType strippedType = SelectQueryHelper.stripNullable(dataType);
        final int precision, scale;
        switch (strippedType.getType()) {
            case DECIMAL:
                DecimalDataType decimalDataType = (DecimalDataType) strippedType;
                precision = decimalDataType.getP();
                scale = decimalDataType.getS();
                break;
            case DECIMAL32:
                precision = ClickHouseDataType.Decimal32.getMaxPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;
            case DECIMAL64:
                precision = ClickHouseDataType.Decimal64.getMaxPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;
            case DECIMAL128:
                precision = ClickHouseDataType.Decimal128.getMaxPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;
            case DATE_TIME64:
                precision = ClickHouseDataType.DateTime64.getMaxPrecision();
                scale = ClickHouseDataType.DateTime64.getMaxScale();
                break;


            default:
                throw new UnsupportedOperationException();
        }

        StringBuilder decimalValueAsStr = new StringBuilder();
        int digitsExcludeFraction = precision - scale;
        for (int i = 0; i < digitsExcludeFraction; i++) {
            decimalValueAsStr.append('9');
        }

        for (int i = 0; i < scale; i++) {
            if (i == 0)
                decimalValueAsStr.append('.');

            decimalValueAsStr.append('9');
        }

        return new BigDecimal(decimalValueAsStr.toString());
    }

    public static List<ColumnDeclaration> doubleMergeColumns(List<ColumnDeclaration> expectedTable, List<ColumnDeclaration> actualTable) {
        return mergeColumns(actualTable, mergeColumns(expectedTable, actualTable));
    }

    public static List<ColumnDeclaration> mergeColumns(List<ColumnDeclaration> expectedTable, List<ColumnDeclaration> actualTable) {
        List<ColumnDeclaration> actualColumnsDefinition = convertExColumns(ColumnDeclarationEx.getColumnsDeepForDefinition(toExColumnsUnchecked(actualTable)));
        TableSchemaMerger schemaMerger = new TableSchemaMerger(null,  expectedTable, actualColumnsDefinition, false);
        if (schemaMerger.mergeSchema()) {
            return schemaMerger.getFilterColumns();
        }
        return expectedTable;
    }

    public static List<ColumnDeclarationEx> getInsertColumns(TableDeclaration declaration, List<ColumnDeclarationEx> columnDeclarations) {
        final List<ColumnDeclarationEx> insertColumns;
        List<ColumnDeclarationEx> typeColumnsCopy = columnDeclarations.stream().map(ColumnDeclarationEx::deepCopy).collect(Collectors.toList());
        List<ColumnDeclarationEx> tableColumnsCopy = declaration.getColumns()
                .stream()
                .map(c -> (ColumnDeclarationEx)c)
                .map(ColumnDeclarationEx::deepCopy)
                .collect(Collectors.toList());
        insertColumns = toExColumnsUnchecked(ClickhouseUtil.doubleMergeColumns(convertExColumns(typeColumnsCopy), convertExColumns(tableColumnsCopy)));
        setStatementIndex(insertColumns);
        return insertColumns;
    }
    public static void setStatementIndex(List<ColumnDeclarationEx> columns) {
        List<ColumnDeclarationEx> columnsDeep = ColumnDeclarationEx.getColumnsDeep(columns);
        int statementIndex = 1;
        for (ColumnDeclarationEx column : columnsDeep) {
            column.setStatementIndex(statementIndex++);
        }
    }

    public static Set<String> getAvailableFieldsNames(List<ColumnDeclarationEx> clickhouseContext) {
        return clickhouseContext.stream().map(ColumnDeclaration::getDbColumnName).collect(Collectors.toSet());
    }

    public static List<ColumnDeclaration> convertExColumns(List<ColumnDeclarationEx> columns) {
        return columns.stream().map(c -> (ColumnDeclaration) c).collect(Collectors.toList());
    }

    public static List<ColumnDeclarationEx> toExColumnsUnchecked(List<ColumnDeclaration> columns) {
        return columns.stream().map(c -> (ColumnDeclarationEx) c).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        System.out.println(GMT.formatNanos(DATETIME_64_MAX_VALUE));
        System.out.println(GMT.formatNanos(DATETIME_64_MIN_VALUE));
        System.out.println(DATETIME_64_MAX_VALUE);
    }
}