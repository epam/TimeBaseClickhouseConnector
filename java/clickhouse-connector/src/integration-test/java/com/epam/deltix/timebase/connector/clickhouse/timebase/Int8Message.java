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


public class Int8Message extends InstrumentMessage {
    public static final String CLASS_NAME = Int8Message.class.getName();

    private byte int8Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT8,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public byte getInt8Field() {
        return int8Field;
    }

    public void setInt8Field(byte int8Field) {
        this.int8Field = int8Field;
    }
}