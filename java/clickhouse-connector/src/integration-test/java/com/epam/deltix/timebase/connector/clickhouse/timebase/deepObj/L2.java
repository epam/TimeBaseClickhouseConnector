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
import com.epam.deltix.util.collections.generated.ObjectArrayList;

import java.util.List;

@SchemaElement(
        name = "deltix.clickhouse.timebase.L2"
)
public class L2 extends InstrumentMessage {

    @SchemaElement
    @SchemaArrayType(
            elementDataType = SchemaDataType.OBJECT,
            elementTypes = {L3.class}
    )
    public ObjectArrayList<InstrumentMessage> l2ArrObj = new ObjectArrayList<>();


    @SchemaElement
    @SchemaType(dataType = SchemaDataType.OBJECT, nestedTypes = {L3.class})
    public InstrumentMessage l2obj;

    @SchemaElement
    @SchemaType()
    public String l2simple;

    public L2() {
    }

    public L2(InstrumentMessage l2obj, String l2simple, List<InstrumentMessage> l2ArrObj) {
        this.l2ArrObj.addAll(l2ArrObj);
        this.l2obj = l2obj;
        this.l2simple = l2simple;
    }
}