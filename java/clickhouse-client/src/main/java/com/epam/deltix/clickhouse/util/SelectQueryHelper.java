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
package com.epam.deltix.clickhouse.util;

import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.clickhouse.selector.definitions.*;
import com.epam.deltix.clickhouse.selector.params.*;
import com.epam.deltix.clickhouse.writer.codec.CodecUtil;
import com.epam.deltix.clickhouse.ClickhouseClient;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import com.epam.deltix.clickhouse.selector.QuerySource;
import com.epam.deltix.clickhouse.selector.SelectBuilder;
import com.epam.deltix.clickhouse.selector.WhereOperator;
import com.epam.deltix.clickhouse.selector.filters.ApplicableTypes;
import com.epam.deltix.clickhouse.selector.filters.FilterType;
import com.epam.deltix.gflog.api.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SelectQueryHelper {
    private static final Log LOG = LogFactory.getLog(SelectQueryHelper.class);

    public static SelectBuilder buildQuery(QuerySource querySource,
                                           SearchRequestDefinition<String> searchRequest) {
        final Map<String, ColumnDeclaration> columnDeclarations =
                querySource.getExpressions().stream().collect(Collectors.toMap(ColumnDeclaration::getDbColumnName, Function.identity()));

        return buildQuery(querySource, searchRequest, columnDeclarations);
    }

    public static SelectBuilder buildQuery(QuerySource querySource,
                                           SearchRequestDefinition<String> searchRequest,
                                           Map<String, ColumnDeclaration> columnDeclarations) {
        return buildQuery(querySource, searchRequest, columnDeclarations, false);
    }

    public static SelectBuilder buildQuery(QuerySource querySource, SearchRequestDefinition<String> searchRequest,
                                           Map<String, ColumnDeclaration> columnDeclarations, boolean distinct) {
        validateSearchRequest(searchRequest, columnDeclarations);

        List<String> expressions = searchRequest.getExpressions();
        FilterNode<String> filter = searchRequest.getFilter();
        List<? extends SortDefinition<String>> sorts = searchRequest.getSorts();
        List<String> groupByExpressions = searchRequest.getGroupByExpressions();
        Long skip = searchRequest.getSkip();
        Integer take = searchRequest.getTake();

        SelectBuilder selectBuilder = new SelectBuilder();
        querySource.populateFrom(selectBuilder);

        if (expressions != null)
            selectBuilder.expressions(expressions);
        else
            selectBuilder.expressions(SqlQueryHelper.getQueryExpressions(querySource.getExpressions()));

        if (filter != null)
            selectBuilder.where(parseWhereNode(querySource, filter, columnDeclarations));

        if (sorts != null)
            addSorts(selectBuilder, sorts, columnDeclarations);

        if (groupByExpressions != null)
            selectBuilder.groupBy(groupByExpressions);

        if (distinct)
            selectBuilder.distinct();

        if (take != null) {
            if (skip != null)
                selectBuilder.limit(skip.longValue(), take.intValue());
            else
                selectBuilder.limit(take.intValue());
        } else {
            if (skip != null)
                LOG.info().append("Ignoring 'skip' parameter as take was not set.");
        }

        return selectBuilder;
    }

    public static <TEntry> List<TEntry> executeQuery(SelectBuilder sb, ClickhouseClient clickhouseClient, RowMapper<TEntry> rowMapper) {
        return executeQuery(sb.build(), clickhouseClient, sb.getParams(), rowMapper);
    }

    public static <TEntry> List<TEntry> executeQuery(String sql, ClickhouseClient clickhouseClient, RowMapper<TEntry> rowMapper) {
        return executeQuery(sql, clickhouseClient, null, rowMapper);
    }

    public static <TEntry> List<TEntry> executeQuery(String sql, ClickhouseClient clickhouseClient,
                                                     List<SelectParam> params, RowMapper<TEntry> rowMapper) {
        return ErrorHelper.handleClickhouseException(() -> {
                    if (LOG.isDebugEnabled()) {
                        LogEntry logEntry = LOG.debug()
                                .append("Prepared SQL: [")
                                .append(sql);

                        if (params != null)
                            logEntry = logEntry
                                    .append("] (")
                                    .append(params.stream().map(SelectParam::getValue).collect(Collectors.toList()))
                                    .append(")");

                        logEntry.commit();
                    }

                    List<TEntry> entries = new ArrayList<>();
                    PreparedStatement ps;
                    try {
                        Connection connection = clickhouseClient.getConnection();
                        ps = connection.prepareStatement(sql);
                        buildPrepareStatement(params, ps);

                        ResultSet resultSet = ps.executeQuery();

                        int row = 0;
                        while (resultSet.next()) {
                            entries.add(rowMapper.mapRow(resultSet, ++row));
                        }
                    } catch (SQLException e) {
                        LOG.error()
                                .append("SQL error. ")
                                .append(e)
                                .commit();
                        throw e;
                    }

                    return entries;
                }
        );
    }

    public static <TEntry> List<TEntry> buildAndExecuteQuery(QuerySource querySource,
                                                             SearchRequestDefinition<String> searchRequest,
                                                             Map<String, ColumnDeclaration> columnDeclarations,
                                                             ClickhouseClient clickhouseClient, RowMapper<TEntry> rowMapper) {
        return buildAndExecuteQuery(querySource, searchRequest, columnDeclarations, false, clickhouseClient, rowMapper);
    }

    public static <TEntry> List<TEntry> buildAndExecuteQuery(QuerySource querySource, SearchRequestDefinition<String> searchRequest,
                                                             Map<String, ColumnDeclaration> columnDeclarations, boolean distinct,
                                                             ClickhouseClient clickhouseClient, RowMapper<TEntry> rowMapper) {
        SelectBuilder sb = buildQuery(querySource, searchRequest, columnDeclarations, distinct);
        return executeQuery(sb, clickhouseClient, rowMapper);
    }

    public static SelectBuilder.WhereNode parseWhereNode(QuerySource querySource,
                                                         FilterNode<String> filterNode,
                                                         Map<String, ColumnDeclaration> columnDeclarations) {
        if (filterNode instanceof AndFilterDefinition) {
            SelectBuilder.WhereNode[] childNodes = ((AndFilterDefinition<String>) filterNode).getAnd().stream()
                    .map(item -> parseWhereNode(querySource, item, columnDeclarations))
                    .toArray(SelectBuilder.WhereNode[]::new);
            return new SelectBuilder.WhereMultipleExpression(WhereOperator.AND, childNodes);
        } else if (filterNode instanceof OrFilterDefinition) {
            SelectBuilder.WhereNode[] childNodes = ((OrFilterDefinition<String>) filterNode).getOr().stream()
                    .map(item -> parseWhereNode(querySource, item, columnDeclarations))
                    .toArray(SelectBuilder.WhereNode[]::new);
            return new SelectBuilder.WhereMultipleExpression(WhereOperator.OR, childNodes);
        } else if (filterNode instanceof SubqueryFilterDefinition) {
            return buildSubqueryFilter(querySource, (SubqueryFilterDefinition<String>) filterNode, columnDeclarations);
        } else if (filterNode instanceof FilterDefinition) {
            return buildFilter((FilterDefinition<String>) filterNode, columnDeclarations);
        } else {
            throw new IllegalArgumentException("Illegal filter definition.");
        }
    }

    public static SelectBuilder.WhereNode buildSubqueryFilter(QuerySource querySource,
                                                              SubqueryFilterDefinition<String> filter,
                                                              Map<String, ColumnDeclaration> columnDeclarations) {
        SelectBuilder subFilter = buildQuery(querySource, filter.getFilterValue(), columnDeclarations);

        return new SelectBuilder.WhereSubqueryExpression(filter.getFieldExpression(), buildFilterType(filter.getFilterType()), subFilter);
    }

    public static SelectBuilder.WhereNode buildFilter(FilterDefinition<String> filter, Map<String, ColumnDeclaration> columnDeclarations) {
        FilterType filterType = filter.getFilterType();
        String operator = buildFilterType(filterType);

        String fieldName = filter.getFieldExpression();
        List<String> filterValue = filter.getFilterValue();
        ColumnDeclaration column = columnDeclarations.get(fieldName);

        try {
            return addSqlFilterExpression(filterValue, filterType, column, operator, fieldName);
        } catch (NumberFormatException | DateTimeException ex) {
            throw illegalFilterValue(filter);
        }
    }

    public static void buildPrepareStatement(List<SelectParam> params, PreparedStatement pstmt) throws SQLException {
        if (params != null) {
            int index = 1;
            for (SelectParam selectParam : params) {
                switch (selectParam.getFilterType()) {
                    case ENUM8:
                        pstmt.setString(index++, ((Enum8Param) selectParam).getEnumValue());
                        break;
                    case ENUM16:
                        pstmt.setString(index++, ((Enum16Param) selectParam).getEnumValue());
                        break;
                    case STRING:
                    case FIXED_STRING:
                        pstmt.setString(index++, ((StringParam) selectParam).getStringValue());
                        break;
                    case UINT8:
                    case UINT16:
                    case UINT32:
                    case UINT64:
                        pstmt.setLong(index++, ((UInt64Param) selectParam).getUint64Value());
                        break;
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        pstmt.setLong(index++, ((Int64Param) selectParam).getInt64Value());
                        break;
                    case FLOAT32:
                    case FLOAT64:
                        pstmt.setDouble(index++, ((Float64Param) selectParam).getFloat64Value());
                        break;

                    case DECIMAL:
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        pstmt.setBigDecimal(index++, ((DecimalParam) selectParam).getDecimalValue());
                        break;

                    case DATE:
                        Date sqlDate = Date.valueOf(((DateParam) selectParam).getDateValue());
                        pstmt.setDate(index++, sqlDate);
                        break;
                    case DATE_TIME:
                    case DATE_TIME64:
                        // @MS This is a work-around for lack of support of new DateTime64 fields in clickhouse client
                        // We have to format string manually to preserve millisecond resolution
                        //pstmt.setTimestamp(index++, new Timestamp(((DateTimeParam) selectParam).getDateTimeValue().toEpochMilli()));
                        pstmt.setString(index++, CodecUtil.TIMESTAMP_FORMAT_MS.format(new java.util.Date(((DateTimeParam) selectParam).getDateTimeValue().toEpochMilli())));
                        break;

                    default:
                        throw unsupportedFilterException(selectParam.getFilterType(), selectParam.getValue().toString());
                }
            }
        }
    }

    public static void validateSearchRequest(SearchRequestDefinition<String> searchRequest, Map<String, ColumnDeclaration> columnDeclarations) {
        validateFilterNode(searchRequest.getFilter(), columnDeclarations);
    }

    public static void validateFilterNode(FilterNode<String> filterNode, Map<String, ColumnDeclaration> columnDeclarations) {
        if (filterNode instanceof AndFilterDefinition) {
            List<? extends FilterNode<String>> childNodes = ((AndFilterDefinition<String>) filterNode).getAnd();
            checkFilterSize(childNodes, AndFilterDefinition.class.getSimpleName());
            for (FilterNode<String> node : childNodes) {
                validateFilterNode(node, columnDeclarations);
            }
        } else if (filterNode instanceof OrFilterDefinition) {
            List<? extends FilterNode<String>> childNodes = ((OrFilterDefinition<String>) filterNode).getOr();
            checkFilterSize(childNodes, OrFilterDefinition.class.getSimpleName());
            for (FilterNode<String> node : childNodes) {
                validateFilterNode(node, columnDeclarations);
            }
        } else if (filterNode instanceof SubqueryFilterDefinition) {
            validateSubqueryFilterDefinition((SubqueryFilterDefinition<String>) filterNode, columnDeclarations);
        } else if (filterNode instanceof FilterDefinition) {
            validateFilterDefinition((FilterDefinition<String>) filterNode, columnDeclarations);
        }
    }

    public static void validateSubqueryFilterDefinition(SubqueryFilterDefinition<String> filter, Map<String, ColumnDeclaration> columnDeclarations) {
        String fieldName = filter.getFieldExpression();
        FilterType filterType = filter.getFilterType();
        SearchRequestDefinition<String> filterValue = filter.getFilterValue();

        if (fieldName == null || filterType == null || filterValue == null)
            throw illegalFilterValue();

        ColumnDeclaration column = columnDeclarations.get(fieldName);

        if (column == null)
            throw new IllegalArgumentException(String.format("Illegal filter for unknown column '%s'.", fieldName));

        //todo support
        switch (filterType) {
            case CONTAINS:
            case NOT_CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                throw new IllegalArgumentException(String.format("Unsupported filter: '%s'", filterType));
        }

        validateSearchRequest(filterValue, columnDeclarations);
    }

    public static void validateFilterDefinition(FilterDefinition<String> filter, Map<String, ColumnDeclaration> columnDeclarations) {
        String fieldName = filter.getFieldExpression();
        FilterType filterType = filter.getFilterType();
        List<String> filterValue = filter.getFilterValue();

        if (fieldName == null || filterType == null || filterValue == null)
            throw illegalFilterValue();

        ColumnDeclaration column = columnDeclarations.get(fieldName);

        if (column == null)
            throw new IllegalArgumentException(String.format("Illegal filter for unknown column '%s'.", fieldName));

        DataTypes columnStrippedType = stripNullable(column.getDbDataType()).getType();

        if (!isAcceptableFilter(filterType, columnStrippedType))
            throw unsupportedFilterException(filterType, column, stripNullable(column.getDbDataType()).getType());

        if (columnStrippedType.equals(DataTypes.STRING) ||
                columnStrippedType.equals(DataTypes.FIXED_STRING)) {
            for (String item : filterValue) {
                if (item == null)
                    throw illegalFilterValue(filter);
            }
        } else {
            for (String item : filterValue) {
                if (StringUtils.isBlank(item))
                    throw illegalFilterValue(filter);
            }
        }

        if (!isAcceptableFilterValue(filterValue, filterType))
            throw illegalFilterValue(filter);
    }


    public static boolean isAcceptableFilter(FilterType filterType, DataTypes columnType) {
        try {
            ApplicableTypes applicableTypes = FilterType.class.getField(filterType.name()).getAnnotation(ApplicableTypes.class);
            return Arrays.stream(applicableTypes.value()).anyMatch(item -> isFilterApplicable(item, columnType));
        } catch (NoSuchFieldException | SecurityException e) {
            LOG.error()
                    .append("Cannot find member '")
                    .append(filterType.name())
                    .append("' in FilterType class")
                    .commit();

            throw new IllegalStateException();
        }
    }

    public static boolean isFilterApplicable(DataTypes fieldType, DataTypes columnType) {
        return fieldType == columnType;
    }

    public static boolean isAcceptableFilterValue(List<String> filterValue, FilterType filterType) {
        return (filterType.equals(FilterType.IN) || filterType.equals(FilterType.NOT_IN)) ?
                filterValue.size() >= 1 : filterValue.size() == 1;
    }

    public static void checkFilterSize(List<? extends FilterNode<String>> childNodes, String filterType) {
        if (childNodes.size() <= 1)
            throw illegalArgumentException(filterType, childNodes.size());
    }

    public static SqlDataType stripNullable(SqlDataType columnType) {
        if (columnType instanceof NullableDataType) {
            columnType = ((NullableDataType)columnType).getNestedType();
        }

        return columnType;
    }

    public static Pair<Integer, Integer> calculateDecimalPrecisionScale(SqlDataType columnType) {
        return Pair.of(((DecimalDataType) columnType).getP(), ((DecimalDataType) columnType).getS());
    }

    public static int calculateDecimalDigitByP(int p) {
        int digit = 128;

        if (p >= 1 && p <= 9)
            digit = 32;
        else if (p >= 10 && p <= 18)
            digit = 64;

        return digit;
    }

    public static int calculateDecimalScaleByType(SqlDataType columnType) {
        return ((BaseDecimalDataType) columnType).getS();
    }

    public static String toDecimal(SqlDataType columnType) {
        if (columnType.getType().equals(DataTypes.DECIMAL) ) {
            Pair<Integer, Integer> decimalPS = calculateDecimalPrecisionScale(columnType);
            int digit = calculateDecimalDigitByP(decimalPS.getLeft());

            return String.format("toDecimal%d(?, %d)", digit, decimalPS.getRight());
        }
        int scale = calculateDecimalScaleByType(columnType);
        return String.format("to%s(?, %d)", columnType.getSqlDefinition(), scale);
    }

    public static String wrapInToStringExpression(final String expression) {
        return String.format("toString(%s)", expression);
    }


    private static String buildFilterType(FilterType filterType) {
        switch (filterType) {
            case EQUAL:
                return "=";
            case NOT_EQUAL:
                return "!=";
            case GREATER:
                return ">";
            case GREATER_OR_EQUAL:
                return ">=";
            case LESS:
                return "<";
            case LESS_OR_EQUAL:
                return "<=";
            case IN:
                return "IN";
            case NOT_IN:
                return "NOT IN";
            case CONTAINS:
            case NOT_CONTAINS:
            case STARTS_WITH:
            case ENDS_WITH:
                return "";

            default:
                throw new UnsupportedOperationException(String.format("Unknown filter '%s'", filterType));
        }
    }

    private static SelectBuilder.WhereNode addSqlFilterExpression(List<String> gridFilterExpressions, FilterType filterType,
                                                                  ColumnDeclaration columnDeclaration, String operator, String fieldName) {
        SqlDataType columnType = stripNullable(columnDeclaration.getDbDataType());

        DataTypes dataType = columnType.getType();
        switch (dataType) {
            case ENUM8:
                Enum8DataType enum8DataType = (Enum8DataType)columnType;
                gridFilterExpressions.forEach(value -> {
                    if (enum8DataType.getValues().stream().noneMatch(item -> item.getName().equals(value)))
                        throw unsupportedFilterValue(value, columnDeclaration, columnType.getType());
                });
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(Enum8Param::new)
                        .toArray(SelectParam[]::new));

            case ENUM16:
                Enum16DataType enum16DataType = (Enum16DataType)columnType;
                gridFilterExpressions.forEach(value -> {
                    if (enum16DataType.getValues().stream().noneMatch(item -> item.getName().equals(value)))
                        throw unsupportedFilterValue(value, columnDeclaration, columnType.getType());
                });
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(Enum16Param::new)
                        .toArray(SelectParam[]::new));

            case STRING:
            case FIXED_STRING:
                return calculateStringParam(gridFilterExpressions, filterType, fieldName, operator);

            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(item ->
                        getIntOrFloatParam(() -> new UInt64Param(Long.parseUnsignedLong(item)), item)).toArray(SelectParam[]::new));

            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(item ->
                        getIntOrFloatParam(() -> new Int64Param(Long.parseLong(item)), item)).toArray(SelectParam[]::new));

            case FLOAT32:
            case FLOAT64:
                return calculateFloatParam(gridFilterExpressions, filterType, fieldName, operator);

            case DECIMAL:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                String toDecimal = toDecimal(columnType);
                return calculateDecimalParam(gridFilterExpressions, filterType, fieldName, toDecimal, operator);

            case DATE:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(item ->
                        new DateParam(LocalDate.parse(item))).toArray(SelectParam[]::new));

            case DATE_TIME:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(item ->
                        new DateTimeParam(Instant.parse(item))).toArray(SelectParam[]::new));

            case DATE_TIME64:
                return calculateDateTime64Param(gridFilterExpressions, filterType, fieldName, operator, ((DateTime64DataType) columnType).getPrecision() );

            default:
                throw unsupportedFilterException(columnDeclaration, columnType.getType());

        }
    }

    private static SelectBuilder.WhereNode calculateFloatParam(List<String> gridFilterExpressions, FilterType filterType,
                                                               String fieldName, String operator) {
        double float64Param = Double.parseDouble(gridFilterExpressions.get(0));
        double epsilon = getEpsilon(float64Param);
        switch (filterType) {
            case EQUAL:
                return new SelectBuilder.WhereMultipleExpression(WhereOperator.AND,
                        new SelectBuilder.WhereExpression(fieldName, ">", new Float64Param[] {new Float64Param(float64Param - epsilon)}),
                        new SelectBuilder.WhereExpression(fieldName, "<", new Float64Param[] {new Float64Param(float64Param + epsilon)}));

            case NOT_EQUAL:
                return new SelectBuilder.WhereExpression(fieldName, operator, new Float64Param[] {
                        new Float64Param(float64Param) });

            case GREATER:
            case LESS_OR_EQUAL:
                return new SelectBuilder.WhereExpression(fieldName, operator, new Float64Param[] {
                        new Float64Param(float64Param + epsilon) });

            case LESS:
            case GREATER_OR_EQUAL:
                return new SelectBuilder.WhereExpression(fieldName, operator, new Float64Param[] {
                        new Float64Param(float64Param - epsilon) });

            case IN:
            case NOT_IN:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(item ->
                        new Float64Param(Double.parseDouble(item))).toArray(SelectParam[]::new));

            default:
                throw new UnsupportedOperationException(String.format("Unknown filter '%s'", filterType));
        }
    }

    private static SelectBuilder.WhereNode calculateDecimalParam(List<String> gridFilterExpressions, FilterType filterType, String fieldName,
                                                                 String toDecimal, String operator) {
        BigDecimal decimalParam = new BigDecimal(gridFilterExpressions.get(0));
        switch (filterType) {
            case NOT_EQUAL:
                // 'IS NULL' need to fix bug with empty result when use toDecimal(P, S) function after not equal operator
                fieldName = String.format("%s IS NULL OR %s %s %s", fieldName, fieldName, operator, toDecimal);
                return new SelectBuilder.WhereRawExpression(fieldName, new DecimalParam[] { new DecimalParam(decimalParam) });

            case EQUAL:
            case GREATER:
            case GREATER_OR_EQUAL:
            case LESS:
            case LESS_OR_EQUAL:
                fieldName = String.format("%s %s %s", fieldName, operator, toDecimal);
                return new SelectBuilder.WhereRawExpression(fieldName, new DecimalParam[] { new DecimalParam(decimalParam) });

            default:
                throw new UnsupportedOperationException(String.format("Unknown filter '%s'", filterType));
        }
    }

    private static SelectBuilder.WhereNode calculateStringParam(List<String> gridFilterExpressions, FilterType filterType,
                                                                String fieldName, String operator) {
        StringParam stringParam = new StringParam(gridFilterExpressions.get(0));
        switch (filterType) {
            case EQUAL:
                fieldName = String.format("positionCaseInsensitive(%s, ?) == 1 AND length(%s) == length(?)", fieldName, fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam, stringParam });

            case NOT_EQUAL:
                fieldName = String.format("positionCaseInsensitive(%s, ?) != 1 OR length(%s) != length(?)", fieldName, fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam, stringParam });

            case CONTAINS:
                fieldName = String.format("positionCaseInsensitive(%s, ?) != 0", fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam });

            case NOT_CONTAINS:
                fieldName = String.format("positionCaseInsensitive(%s, ?) == 0", fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam });

            case STARTS_WITH:
                fieldName = String.format("positionCaseInsensitive(%s, ?) == 1", fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam });

            case ENDS_WITH:
                fieldName = String.format("positionCaseInsensitive(%s, ?) != 0 AND positionCaseInsensitive(%s, ?) = length(%s) - length(?) + 1", fieldName, fieldName, fieldName);
                return new SelectBuilder.WhereRawExpression(fieldName, new StringParam[] { stringParam, stringParam, stringParam });

            case IN:
            case NOT_IN:
                return new SelectBuilder.WhereExpression(fieldName, operator, gridFilterExpressions.stream().map(StringParam::new)
                        .toArray(SelectParam[]::new));

            default:
                throw new UnsupportedOperationException(String.format("Unknown filter '%s'", filterType));
        }
    }

    private static SelectBuilder.WhereNode calculateDateTime64Param(List<String> gridFilterExpressions, FilterType filterType,
                                                                    String fieldName, String operator, int precision) {
        // @MS This is a work-around for lack of support of new DateTime64 fields in clickhouse client
        // We have to use toDateTime64 function https://github.com/ClickHouse/ClickHouse/issues/9008

        Instant dateTimeParam = Instant.parse(gridFilterExpressions.get(0));
        switch (filterType) {
            case EQUAL:
            case NOT_EQUAL:
            case GREATER:
            case GREATER_OR_EQUAL:
            case LESS:
            case LESS_OR_EQUAL:
                fieldName = String.format("%s %s toDateTime64(?, %d)", fieldName, operator, precision);
                return new SelectBuilder.WhereRawExpression(fieldName, new DateTimeParam[] { new DateTimeParam(dateTimeParam) });

            default:
                throw new UnsupportedOperationException(String.format("Unknown filter '%s'", filterType));
        }
    }

    private static SelectParam getIntOrFloatParam(Supplier<SelectParam> intSupplier, String value) {
        try {
            return intSupplier.get();
        } catch (NumberFormatException e) {
            return new Float64Param(Double.parseDouble(value));
        }
    }

    // Largest double-precision floating-point number such that 1 + EPSILON is numerically equal to 1.
    // Calculate relative epsilon using ULP(unit of least precision) to minimize floating point error.
    // Detailed article on this topic - https://bitbashing.io/comparing-floats.html
    private static double getEpsilon(double value) {
        // Handle the near-zero case.
        if (value < 0.0001)
            return 1.E-14;

        double ulp = Math.ulp(value);
        return ulp * 1000;
    }

    private static void addSorts(SelectBuilder selectBuilder,
                                 List<? extends SortDefinition<String>> sorts,
                                 Map<String, ColumnDeclaration> columnDeclarations) {
        sorts.forEach(sort -> {
            String fieldName = sort.getFieldName();

            ColumnDeclaration column = columnDeclarations.get(fieldName);
            if (column != null) {
                SqlDataType columnType = stripNullable(column.getDbDataType());
                DataTypes dataType = columnType.getType();

                if (dataType == DataTypes.ENUM8 || dataType == DataTypes.ENUM16)
                    fieldName = wrapInToStringExpression(fieldName);
            }

            selectBuilder.orderBy(fieldName, sort.getSortType().toString());
        });
    }

    private static RuntimeException unsupportedFilterValue(String gridFilterExpression, ColumnDeclaration columnDeclaration, DataTypes dataType) {
        throw new IllegalArgumentException(String.format("Unexpected filter value '%s' for column %s (%s)", gridFilterExpression, columnDeclaration.getDbColumnName(), dataType));
    }

    private static RuntimeException unsupportedFilterException(ColumnDeclaration columnDeclaration, DataTypes dataType) {
        throw new IllegalArgumentException(String.format("Unsupported filter type for column %s (%s)", columnDeclaration.getDbColumnName(), dataType));
    }


    private static RuntimeException unsupportedFilterException(FilterType filterType, ColumnDeclaration columnDeclaration, DataTypes dataType) {
        throw new IllegalArgumentException(String.format("Unsupported filter type '%s' for field %s (%s)", filterType.name(), columnDeclaration.getDbColumnName(), dataType));
    }

    private static RuntimeException unsupportedFilterException(DataTypes dataType, String fieldName) {
        throw new IllegalArgumentException(String.format("Unsupported data type '%s' for field %s", dataType, fieldName));
    }

    private static RuntimeException illegalFilterValue(FilterDefinition<String> filter) {
        throw new IllegalArgumentException(String.format("Illegal value for filter '%s' (%s): %s", filter.getFieldExpression(), filter.getFilterType().name(), filter.getFilterValue()));
    }

    private static RuntimeException illegalFilterValue() {
        throw new IllegalArgumentException("Filter values should not be null");
    }

    private static RuntimeException illegalArgumentException(String filterType, int argumentCount) {
        throw new IllegalArgumentException(String.format("Illegal count of arguments for filter type %s." +
                " Should be greater than 1. Actual count: %d ", filterType, argumentCount));
    }
}