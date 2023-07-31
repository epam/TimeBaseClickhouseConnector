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


public class NullableFloat64Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableFloat64Message.class.getName();

    private double nullableFloat64Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_FIXED_DOUBLE,
            dataType = SchemaDataType.FLOAT
    )
    public double getNullableFloat64Field() {
        return nullableFloat64Field;
    }

    public void setNullableFloat64Field(double nullableFloat64Field) {
        this.nullableFloat64Field = nullableFloat64Field;
    }
}