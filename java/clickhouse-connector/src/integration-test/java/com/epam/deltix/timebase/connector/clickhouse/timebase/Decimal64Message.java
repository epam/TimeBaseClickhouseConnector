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

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import com.epam.deltix.timebase.messages.*;


public class Decimal64Message extends InstrumentMessage {
    public static final String CLASS_NAME = Decimal64Message.class.getName();

    private long decimal64Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_DECIMAL64,
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public long getDecimal64Field() {
        return decimal64Field;
    }

    public void setDecimal64Field(long decimal64Field) {
        this.decimal64Field = decimal64Field;
    }
}