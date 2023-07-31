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

public class NullableBooleanMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableBooleanMessage.class.getName();

    private byte nullableBooleanField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.BOOLEAN
    )
    public boolean isNullableBooleanField() {
        return nullableBooleanField == 1;
    }

    public void setNullableBooleanField(boolean nullableBooleanField) {
        this.nullableBooleanField = (byte)(nullableBooleanField ? 1 : 0);
    }

    public boolean hasNullableBooleanField() {
        return nullableBooleanField != TypeConstants.BOOLEAN_NULL;
    }

    public void nullifyNullableBooleanField() {
        this.nullableBooleanField = TypeConstants.BOOLEAN_NULL;
    }
}