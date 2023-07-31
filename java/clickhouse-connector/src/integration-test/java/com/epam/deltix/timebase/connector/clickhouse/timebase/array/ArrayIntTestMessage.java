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
package com.epam.deltix.timebase.connector.clickhouse.timebase.array;

import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.util.collections.generated.IntegerArrayList;


@SchemaElement()
public class ArrayIntTestMessage extends InstrumentMessage {
    public static final String CLASS_NAME = ArrayIntTestMessage.class.getName();

    private IntegerArrayList arrayValue;
    private IntegerArrayList arrayNullableValue;
    private IntegerArrayList nullableArrayValue;
    private IntegerArrayList nullableArrayNullableValue;


    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            isElementNullable = false,
            elementEncoding = "INT32",
            elementDataType = SchemaDataType.INTEGER
    )
    public IntegerArrayList getArrayValue() {
        return arrayValue;
    }

    public void setArrayValue(IntegerArrayList arrayValue) {
        this.arrayValue = arrayValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            isElementNullable = true,
            elementEncoding = "INT32",
            elementDataType = SchemaDataType.INTEGER
    )
    public IntegerArrayList getArrayNullableValue() {
        return arrayNullableValue;
    }

    public void setArrayNullableValue(IntegerArrayList arrayNullableValue) {
        this.arrayNullableValue = arrayNullableValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = true,
            isElementNullable = false,
            elementEncoding = "INT32",
            elementDataType = SchemaDataType.INTEGER
    )
    public IntegerArrayList getNullableArrayValue() {
        return nullableArrayValue;
    }

    public void setNullableArrayValue(IntegerArrayList nullableArrayValue) {
        this.nullableArrayValue = nullableArrayValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = true,
            isElementNullable = true,
            elementEncoding = "INT32",
            elementDataType = SchemaDataType.INTEGER
    )
    public IntegerArrayList getNullableArrayNullableValue() {
        return nullableArrayNullableValue;
    }

    public void setNullableArrayNullableValue(IntegerArrayList nullableArrayNullableValue) {
        this.nullableArrayNullableValue = nullableArrayNullableValue;
    }
}