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
package com.epam.deltix.timebase.connector.clickhouse.timebase.callinfo;

import com.epam.deltix.timebase.messages.*;
import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;




import com.epam.deltix.timebase.connector.clickhouse.timebase.TestEnum;
import com.epam.deltix.util.collections.generated.ObjectArrayList;


@SchemaElement(name = "com.epam.model.LocationQuantity")
public class LocationQuantity extends InstrumentMessage {


    @SchemaElement
    @SchemaType(isNullable = false)
    public String Location;
    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public long Quantity;
    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            isElementNullable = false,
            elementEncoding = "UTF8",
            elementDataType = SchemaDataType.VARCHAR
    )
    public ObjectArrayList<CharSequence> Users;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.ENUM
    )
    public TestEnum enumField;

    public LocationQuantity() {
    }

    public LocationQuantity(ObjectArrayList<CharSequence> users, long quantity, String location, TestEnum enumField) {
        Users = users;
        Quantity = quantity;
        Location = location;
        this.enumField = enumField;
    }
}