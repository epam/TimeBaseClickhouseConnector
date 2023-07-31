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
package com.epam.deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.timebase.messages.*;




import com.epam.deltix.util.collections.generated.LongArrayList;

@SchemaElement()
public class VarcharAlphanumericMessage extends InstrumentMessage {
    public static final String CLASS_NAME = VarcharAlphanumericMessage.class.getName();


    private long alphanumeric;

    private LongArrayList alphanumericArray;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.VARCHAR,
            encoding = "ALPHANUMERIC(10)"
    )
    public long getAlphanumeric() {
        return alphanumeric;
    }

    public void setAlphanumeric(long alphanumeric) {
        this.alphanumeric = alphanumeric;
    }

    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            elementEncoding = "ALPHANUMERIC(10)",
            elementDataType = SchemaDataType.VARCHAR
    )
    public LongArrayList getAlphanumericArray() {
        return alphanumericArray;
    }

    public void setAlphanumericArray(LongArrayList alphanumericArray) {
        this.alphanumericArray = alphanumericArray;
    }
}