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

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import com.epam.deltix.timebase.messages.*;



public class NullableInt16Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableInt16Message.class.getName();

    private short nullableInt16Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT16,
            dataType = SchemaDataType.INTEGER
    )
    public short getNullableInt16Field() {
        return nullableInt16Field;
    }

    public void setNullableInt16Field(short nullableInt16Field) {
        this.nullableInt16Field = nullableInt16Field;
    }
}