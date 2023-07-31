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
import com.epam.deltix.util.collections.generated.BooleanArrayList;
import com.epam.deltix.util.collections.generated.ByteArrayList;


@SchemaElement()
public class ArrayBooleanTestMessage extends InstrumentMessage {
    public static final String CLASS_NAME = ArrayBooleanTestMessage.class.getName();

    private BooleanArrayList arrayValue;
    private ByteArrayList arrayNullableValue;
    private ByteArrayList nullableArrayValue;
    private ByteArrayList nullableArrayNullableValue;


    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            isElementNullable = false,
            elementDataType = SchemaDataType.BOOLEAN
    )
    public BooleanArrayList getArrayValue() {
        return arrayValue;
    }

    public void setArrayValue(BooleanArrayList arrayValue) {
        this.arrayValue = arrayValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            isElementNullable = true,
            elementDataType = SchemaDataType.BOOLEAN
    )
    public ByteArrayList getArrayNullableValue() {
        return arrayNullableValue;
    }

    public void setArrayNullableValue(ByteArrayList arrayNullableValue) {
        this.arrayNullableValue = arrayNullableValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = true,
            isElementNullable = false,
            elementDataType = SchemaDataType.BOOLEAN
    )
    public ByteArrayList getNullableArrayValue() {
        return nullableArrayValue;
    }

    public void setNullableArrayValue(ByteArrayList nullableArrayValue) {
        this.nullableArrayValue = nullableArrayValue;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = true,
            isElementNullable = true,
            elementDataType = SchemaDataType.BOOLEAN
    )
    public ByteArrayList getNullableArrayNullableValue() {
        return nullableArrayNullableValue;
    }

    public void setNullableArrayNullableValue(ByteArrayList nullableArrayNullableValue) {
        this.nullableArrayNullableValue = nullableArrayNullableValue;
    }
}