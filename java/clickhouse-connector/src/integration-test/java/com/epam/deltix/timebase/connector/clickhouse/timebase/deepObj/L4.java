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
package com.epam.deltix.timebase.connector.clickhouse.timebase.deepObj;

import com.epam.deltix.timebase.messages.*;



import com.epam.deltix.timebase.connector.clickhouse.timebase.TestEnum;

@SchemaElement(
        name = "deltix.clickhouse.timebase.L4"
)
public class L4 extends InstrumentMessage {

//    @SchemaElement
//    @SchemaArrayType(
//            isNullable = false,
//            isElementNullable = false,
//            elementEncoding = "UTF8",
//            elementDataType = SchemaDataType.VARCHAR
//    )
//    public ObjectArrayList<CharSequence> l4arr;

    @SchemaElement
    @SchemaType()
    public String l4simple;

    private TestEnum enumField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.ENUM
    )
    public TestEnum getEnumField() {
        return enumField;
    }

    public void setEnumField(TestEnum enumField) {
        this.enumField = enumField;
    }

    public L4() {
    }

    public L4( String l4simple, TestEnum enumField) {
        this.l4simple = l4simple;
        this.enumField = enumField;
    }

}