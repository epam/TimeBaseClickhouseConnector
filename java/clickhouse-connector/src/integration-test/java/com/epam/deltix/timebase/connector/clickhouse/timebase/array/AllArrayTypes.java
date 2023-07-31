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
import com.epam.deltix.timebase.connector.clickhouse.timebase.TestEnum;
import com.epam.deltix.util.collections.generated.*;

public class AllArrayTypes extends InstrumentMessage {

    @SchemaArrayType (
            elementEncoding = "INT8",
            elementDataType = SchemaDataType.INTEGER
    )
    public ByteArrayList byteArray;
    @SchemaArrayType (
            elementEncoding = "INT16",
            elementDataType = SchemaDataType.INTEGER
    )
    public ShortArrayList shortArray;

    @SchemaArrayType (
            elementEncoding = "INT64",
            elementDataType = SchemaDataType.INTEGER
    )
    public LongArrayList longArray;
    @SchemaArrayType (
            elementEncoding = "IEEE64",
            elementDataType = SchemaDataType.FLOAT
    )
    public DoubleArrayList doubleArray;
    @SchemaArrayType (
            elementEncoding = "IEEE32",
            elementDataType = SchemaDataType.FLOAT
    )
    public FloatArrayList floatArray;

    @SchemaElement
    @SchemaArrayType(
            elementDataType = SchemaDataType.TIMESTAMP
    )
    public LongArrayList timestampList;


    @SchemaElement
    @SchemaArrayType(
            elementDataType = SchemaDataType.TIME_OF_DAY
    )
    public IntegerArrayList timeOfDayList;


    @SchemaElement
    @SchemaArrayType(
            elementDataType = SchemaDataType.ENUM,
            elementTypes = TestEnum.class
    )
    public ObjectArrayList<CharSequence> enumArray;

}