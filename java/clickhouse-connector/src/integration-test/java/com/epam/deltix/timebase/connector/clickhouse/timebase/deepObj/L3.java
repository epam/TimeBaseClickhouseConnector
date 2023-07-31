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
import com.epam.deltix.util.collections.generated.ObjectArrayList;

import java.util.List;

@SchemaElement(
        name = "deltix.clickhouse.timebase.L3"
)
public class L3 extends InstrumentMessage {

    @SchemaElement
    @SchemaType(dataType = SchemaDataType.OBJECT, nestedTypes = {L4.class})
    public InstrumentMessage l3obj;

    @SchemaElement
    @SchemaType()
    public String l3simple;

    @SchemaElement
    @SchemaArrayType(
            isNullable = false,
            elementDataType = SchemaDataType.ENUM,
            elementTypes = TestEnum.class
    )
    public ObjectArrayList<CharSequence> enumArray = new ObjectArrayList<>();

    public L3() {
    }

    public L3(InstrumentMessage l3obj, String l3simple, List<String> enumArray) {
        this.l3obj = l3obj;
        this.l3simple = l3simple;
        this.enumArray.addAll(enumArray);
    }


}