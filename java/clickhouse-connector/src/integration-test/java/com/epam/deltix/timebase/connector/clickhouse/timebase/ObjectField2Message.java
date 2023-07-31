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

//public class ObjectField2Message extends InstrumentMessage {
//    public static final String CLASS_NAME = ObjectField2Message.class.getName();
//
//    private String stringField;
//    private ObjectFieldMessage objectField;
//
//
//    @SchemaElement
//    @SchemaType(isNullable = false)
//    public String getStringField() {
//        return stringField;
//    }
//
//    public void setStringField(String stringField) {
//        this.stringField = stringField;
//    }
//
//    @SchemaElement
//    @SchemaType(isNullable = true)
//    public ObjectFieldMessage getObjectField() {
//        return objectField;
//    }
//
//    public void setObjectField(ObjectFieldMessage objectField) {
//        this.objectField = objectField;
//    }
//}

import com.epam.deltix.timebase.messages.*;

@SchemaElement()
public class ObjectField2Message extends InstrumentMessage {
    /**
     *
     */
    @SchemaType( dataType = SchemaDataType.OBJECT, nestedTypes = { ObjectFieldMessage.class  })
    public InstrumentMessage objectField;
    /**
     *
     */
    public String stringField;
    @java.lang.Override
    public InstrumentMessage copyFrom (RecordInfo source)
    {
        super.copyFrom (source);
        if (source instanceof ObjectField2Message)
        {
            final ObjectField2Message obj = (ObjectField2Message)source;
            objectField = obj.objectField;
            stringField = obj.stringField;
        }
        return this;
    }
    @java.lang.Override
    public String toString ()
    {
        return super.toString ()+", "+"objectField"+": "+objectField+", "+"stringField"+": "+stringField;
    }
    @java.lang.Override
    public InstrumentMessage clone ()
    {
        final ObjectField2Message msg = new ObjectField2Message ();
        msg.copyFrom (this);
        return msg;
    }

    @SchemaType( dataType = SchemaDataType.OBJECT, nestedTypes = { ObjectFieldMessage.class  })
    public InstrumentMessage getObjectField() {
        return objectField;
    }

    public void setObjectField(InstrumentMessage objectField) {
        this.objectField = objectField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }
}