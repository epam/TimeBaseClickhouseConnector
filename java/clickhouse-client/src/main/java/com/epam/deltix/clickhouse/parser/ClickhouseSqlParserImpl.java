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
package com.epam.deltix.clickhouse.parser;

import com.epam.deltix.clickhouse.parser.gen.ClickhouseSqlParser;
import com.epam.deltix.clickhouse.parser.gen.ClickhouseSqlParserBaseListener;
import com.epam.deltix.clickhouse.schema.types.*;
import com.epam.deltix.clickhouse.schema.ColumnDeclaration;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ClickhouseSqlParserImpl extends ClickhouseSqlParserBaseListener {

    private int numberOfErrors = 0;
    private SqlDataType dataType = null;

    private final Stack<Object> stack = new Stack<>();


    public int getNumberOfErrors() {
        return numberOfErrors;
    }

    public SqlDataType getDataType() {
        return dataType;
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        ++numberOfErrors;
    }


    @Override
    public void enterRoot(ClickhouseSqlParser.RootContext ctx) {

    }

    @Override
    public void exitRoot(ClickhouseSqlParser.RootContext ctx) {
        dataType = (SqlDataType)stack.pop();
    }

    @Override
    public void enterColumnDefinitionCollectionLiteral(ClickhouseSqlParser.ColumnDefinitionCollectionLiteralContext ctx){ }

    @Override
    public void exitColumnDefinitionCollectionLiteral(ClickhouseSqlParser.ColumnDefinitionCollectionLiteralContext ctx){ }

    @Override
    public void enterColumnDefinitionLiteral(ClickhouseSqlParser.ColumnDefinitionLiteralContext ctx){ }

    @Override
    public void exitColumnDefinitionLiteral(ClickhouseSqlParser.ColumnDefinitionLiteralContext ctx){ }

    @Override
    public void enterColumnTypeLiteral(ClickhouseSqlParser.ColumnTypeLiteralContext ctx){
    }

    @Override
    public void exitColumnTypeLiteral(ClickhouseSqlParser.ColumnTypeLiteralContext ctx){

    }

    @Override
    public void enterPrimitiveColumnTypeLiteral(ClickhouseSqlParser.PrimitiveColumnTypeLiteralContext ctx){
        enterAnyColumnTypeLiteral(ctx);
    }

    @Override
    public void exitPrimitiveColumnTypeLiteral(ClickhouseSqlParser.PrimitiveColumnTypeLiteralContext ctx){
    }

    @Override
    public void enterNestedColumnTypeLiteral(ClickhouseSqlParser.NestedColumnTypeLiteralContext ctx){
        enterAnyColumnTypeLiteral(ctx);
    }

    @Override
    public void exitNestedColumnTypeLiteral(ClickhouseSqlParser.NestedColumnTypeLiteralContext ctx){
        List<ColumnDeclaration> columns = (List<ColumnDeclaration>)stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.NESTED)
            throw unexpectedType(clickhouseType);

        SqlDataType nestedType = new NestedDataType(columns);
        stack.push(nestedType);
    }

    @Override
    public void enterNullableColumnTypeLiteral(ClickhouseSqlParser.NullableColumnTypeLiteralContext ctx){
        enterAnyColumnTypeLiteral(ctx);
    }

    @Override
    public void exitNullableColumnTypeLiteral(ClickhouseSqlParser.NullableColumnTypeLiteralContext ctx){
        SqlDataType nestedType = (SqlDataType)stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.NULLABLE)
            throw unexpectedType(clickhouseType);

        SqlDataType nullableDataType = new NullableDataType(nestedType);
        stack.push(nullableDataType);
    }

    @Override
    public void enterTupleColumnTypeLiteral(ClickhouseSqlParser.TupleColumnTypeLiteralContext ctx){
        enterAnyColumnTypeLiteral(ctx);
    }

    @Override
    public void exitTupleColumnTypeLiteral(ClickhouseSqlParser.TupleColumnTypeLiteralContext ctx){
        List<SqlDataType> nestedTypes = new ArrayList<>();

        Object token = stack.pop();

        while (token instanceof SqlDataType) {
            nestedTypes.add((SqlDataType)token);
            token = stack.pop();
        }

        DataTypes clickhouseType = (DataTypes)token;
        if (clickhouseType != DataTypes.NULLABLE)
            throw unexpectedType(clickhouseType);

        Collections.reverse(nestedTypes);

        SqlDataType tupleDataType = new TupleDataType(nestedTypes);
        stack.push(tupleDataType);
    }

    @Override
    public void enterArrayColumnTypeLiteral(ClickhouseSqlParser.ArrayColumnTypeLiteralContext ctx){
        enterAnyColumnTypeLiteral(ctx);
    }

    @Override
    public void exitArrayColumnTypeLiteral(ClickhouseSqlParser.ArrayColumnTypeLiteralContext ctx){
        SqlDataType nestedType = (SqlDataType)stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.ARRAY)
            throw unexpectedType(clickhouseType);

        SqlDataType arrayDataType = new ArraySqlType(nestedType);
        stack.push(arrayDataType);
    }

    @Override
    public void enterEnumColumnTypeLiteral(ClickhouseSqlParser.EnumColumnTypeLiteralContext ctx){ }

    @Override
    public void exitEnumColumnTypeLiteral(ClickhouseSqlParser.EnumColumnTypeLiteralContext ctx){
    }

    @Override
    public void enterDecimalColumnTypeLiteral(ClickhouseSqlParser.DecimalColumnTypeLiteralContext ctx) { }

    @Override
    public void exitDecimalColumnTypeLiteral(ClickhouseSqlParser.DecimalColumnTypeLiteralContext ctx) {
        int sValue = (Integer) stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        SqlDataType dataType;

        switch (clickhouseType) {
            case DECIMAL32:
                dataType = new Decimal32DataType(sValue);
                break;
            case DECIMAL64:
                dataType = new Decimal64DataType(sValue);
                break;
            case DECIMAL128:
                dataType = new Decimal128DataType(sValue);
                break;
            default:
                throw unexpectedType(clickhouseType);
        }

        stack.push(dataType);
    }

    @Override
    public void enterDecimalRawColumnTypeLiteral(ClickhouseSqlParser.DecimalRawColumnTypeLiteralContext ctx) { }

    @Override
    public void exitDecimalRawColumnTypeLiteral(ClickhouseSqlParser.DecimalRawColumnTypeLiteralContext ctx) {
        int sValue = (Integer) stack.pop();
        int pValue = (Integer) stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.DECIMAL)
            throw unexpectedType(clickhouseType);

        SqlDataType dataType = new DecimalDataType(pValue, sValue);
        stack.push(dataType);
    }


    @Override
    public void enterDecimalPLiteral(ClickhouseSqlParser.DecimalPLiteralContext ctx) {
        Integer enumValue = Integer.parseInt(ctx.getText());
        stack.push(enumValue);
    }

    @Override
    public void exitDecimalPLiteral(ClickhouseSqlParser.DecimalPLiteralContext ctx) { }

    @Override
    public void enterDecimalSLiteral(ClickhouseSqlParser.DecimalSLiteralContext ctx) {
        Integer enumValue = Integer.parseInt(ctx.getText());
        stack.push(enumValue);
    }

    @Override
    public void exitDecimalSLiteral(ClickhouseSqlParser.DecimalSLiteralContext ctx) { }


    @Override
    public void enterFixedStringColumnTypeLiteral(ClickhouseSqlParser.FixedStringColumnTypeLiteralContext ctx){ }

    @Override
    public void exitFixedStringColumnTypeLiteral(ClickhouseSqlParser.FixedStringColumnTypeLiteralContext ctx){
        int lengthValue = (Integer) stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.FIXED_STRING)
            throw unexpectedType(clickhouseType);

        SqlDataType fixedStringDataType = new FixedStringDataType(lengthValue);
        stack.push(fixedStringDataType);
    }

    @Override
    public void enterFixedStringColumnLengthLiteral(ClickhouseSqlParser.FixedStringColumnLengthLiteralContext ctx) {
        Integer enumValue = Integer.parseInt(ctx.getText());
        stack.push(enumValue);
    }

    @Override
    public void exitFixedStringColumnLengthLiteral(ClickhouseSqlParser.FixedStringColumnLengthLiteralContext ctx) { }

    @Override
    public void enterDateTime64ColumnTypeLiteral(ClickhouseSqlParser.DateTime64ColumnTypeLiteralContext ctx) { }

    @Override
    public void exitDateTime64ColumnTypeLiteral(ClickhouseSqlParser.DateTime64ColumnTypeLiteralContext ctx) {
        int precisionValue = (Integer) stack.pop();
        DataTypes clickhouseType = (DataTypes)stack.pop();

        if (clickhouseType != DataTypes.DATE_TIME64)
            throw unexpectedType(clickhouseType);

        SqlDataType dataType = new DateTime64DataType(precisionValue);
        stack.push(dataType);
    }

    @Override
    public void enterDateTime64PrecisionLiteral(ClickhouseSqlParser.DateTime64PrecisionLiteralContext ctx) {
        Integer precisionValue = Integer.parseInt(ctx.getText());
        stack.push(precisionValue);
    }

    @Override
    public void exitDateTime64PrecisionLiteral(ClickhouseSqlParser.DateTime64PrecisionLiteralContext ctx) { }

    @Override
    public void enterSimpleColumnTypeLiteral(ClickhouseSqlParser.SimpleColumnTypeLiteralContext ctx) { }

    @Override
    public void exitSimpleColumnTypeLiteral(ClickhouseSqlParser.SimpleColumnTypeLiteralContext ctx){
        DataTypes clickhouseType = (DataTypes)stack.pop();
        SqlDataType dataType;

        switch (clickhouseType) {
            case INT8:
                dataType = new Int8DataType();
                break;
            case INT16:
                dataType = new Int16DataType();
                break;
            case INT32:
                dataType = new Int32DataType();
                break;
            case INT64:
                dataType = new Int64DataType();
                break;
            case UINT8:
                dataType = new UInt8DataType();
                break;
            case UINT16:
                dataType = new UInt16DataType();
                break;
            case UINT32:
                dataType = new UInt32DataType();
                break;
            case UINT64:
                dataType = new UInt64DataType();
                break;
            case FLOAT32:
                dataType = new Float32DataType();
                break;
            case FLOAT64:
                dataType = new Float64DataType();
                break;
            case DATE:
                dataType = new DateDataType();
                break;
            case DATE_TIME:
                dataType = new DateTimeDataType();
                break;
            case STRING:
                dataType = new StringDataType();
                break;
            default:
                throw unexpectedType(clickhouseType);
        }

        stack.push(dataType);
    }

    @Override
    public void enterEnum16ColumnTypeLiteral(ClickhouseSqlParser.Enum16ColumnTypeLiteralContext ctx) { }

    @Override
    public void exitEnum16ColumnTypeLiteral(ClickhouseSqlParser.Enum16ColumnTypeLiteralContext ctx) {
        List<EnumItem> enumItems = new ArrayList<>();

        Object token = stack.pop();

        while (token instanceof EnumItem) {
            enumItems.add((EnumItem)token);
            token = stack.pop();
        }

        DataTypes clickhouseType = (DataTypes)token;

        if (clickhouseType != DataTypes.ENUM16)
            throw unexpectedType(clickhouseType);

        List<Enum16DataType.Enum16Value> enumValues = IntStream.range(0, enumItems.size())
            .map(i -> enumItems.size() - i - 1)
            .mapToObj(i -> enumItems.get(i))
            .map(item -> new Enum16DataType.Enum16Value(item.enumName, item.enumValue))
            .collect(Collectors.toList());

        SqlDataType enum16DataType = new Enum16DataType(enumValues);
        stack.push(enum16DataType);
    }

    @Override
    public void enterEnum8ColumnTypeLiteral(ClickhouseSqlParser.Enum8ColumnTypeLiteralContext ctx) { }

    @Override
    public void exitEnum8ColumnTypeLiteral(ClickhouseSqlParser.Enum8ColumnTypeLiteralContext ctx) {
        List<EnumItem> enumItems = new ArrayList<>();

        Object token = stack.pop();

        while (token instanceof EnumItem) {
            enumItems.add((EnumItem)token);
            token = stack.pop();
        }

        DataTypes clickhouseType = (DataTypes)token;

        if (clickhouseType != DataTypes.ENUM8)
            throw unexpectedType(clickhouseType);

        List<Enum8DataType.Enum8Value> enumValues = IntStream.range(0, enumItems.size())
            .map(i -> enumItems.size() - i - 1)
            .mapToObj(i -> enumItems.get(i))
            .map(item -> new Enum8DataType.Enum8Value(item.enumName, (byte)item.enumValue))
            .collect(Collectors.toList());

        SqlDataType enum8DataType = new Enum8DataType(enumValues);
        stack.push(enum8DataType);
    }

    @Override
    public void enterEnumItemLiteral(ClickhouseSqlParser.EnumItemLiteralContext ctx){ }

    @Override
    public void exitEnumItemLiteral(ClickhouseSqlParser.EnumItemLiteralContext ctx){
        Short enumValue = (Short)stack.pop();
        String enumName = (String)stack.pop();

        EnumItem enumItem = new EnumItem(enumValue, enumName);
        stack.push(enumItem);
    }

    @Override
    public void enterEnumItemNameLiteral(ClickhouseSqlParser.EnumItemNameLiteralContext ctx) {
        String enumName = ctx.getText();
        stack.push(enumName);
    }

    @Override
    public void exitEnumItemNameLiteral(ClickhouseSqlParser.EnumItemNameLiteralContext ctx) {

    }

    @Override
    public void enterEnumItemNumberLiteral(ClickhouseSqlParser.EnumItemNumberLiteralContext ctx) {
        Short enumValue = Short.parseShort(ctx.getText());
        stack.push(enumValue);
    }

    @Override
    public void exitEnumItemNumberLiteral(ClickhouseSqlParser.EnumItemNumberLiteralContext ctx) {

    }

    private void enterAnyColumnTypeLiteral(ParserRuleContext ctx){
        DataTypes clickhouseType = DataTypes.findByName(ctx.getStart().getText());
        stack.push(clickhouseType);
    }


    private RuntimeException unexpectedType(DataTypes clickhouseType) {
        return new IllegalStateException(String.format("Unexpected type '%s'.", clickhouseType.getSqlDefinition()));
    }

    private class EnumItem {
        public short enumValue;
        public String enumName;

        public EnumItem(short enumValue, String enumName) {
            this.enumValue = enumValue;
            this.enumName = enumName;
        }
    }
}