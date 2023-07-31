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

@SchemaElement()
public class ObjectFieldMessage extends InstrumentMessage {
    /**
     *
     */
    private String stringField;
    /**
     *
     */
    private InstrumentMessage bboField;
    /**
     *
     */
    private InstrumentMessage tradeField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.VARCHAR
    )
    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    @SchemaElement
    @SchemaType(dataType = SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class})
    public InstrumentMessage getBboField() {
        return bboField;
    }

    public void setBboField(InstrumentMessage bboField) {
        this.bboField = bboField;
    }

    @SchemaElement

    @SchemaType(dataType = SchemaDataType.OBJECT, nestedTypes = {TradeTestMessage.class})
    public InstrumentMessage getTradeField() {
        return tradeField;
    }

    public void setTradeField(InstrumentMessage tradeField) {
        this.tradeField = tradeField;
    }



    @java.lang.Override
    public InstrumentMessage copyFrom(RecordInfo source) {
        super.copyFrom(source);
        if (source instanceof ObjectFieldMessage) {
            final ObjectFieldMessage obj = (ObjectFieldMessage) source;
            bboField = obj.bboField;
//            marketField = obj.marketField;
            stringField = obj.stringField;
            tradeField = obj.tradeField;
        }
        return this;
    }

    @java.lang.Override
    public String toString() {
        return super.toString() + ", " + "bboField" + ": " + bboField + ", " /*+ "marketField" + ": " + marketField + ", "*/ + "stringField" + ": " + stringField + ", " + "tradeField" + ": " + tradeField;
    }

    @java.lang.Override
    public InstrumentMessage clone() {
        final ObjectFieldMessage msg = new ObjectFieldMessage();
        msg.copyFrom(this);
        return msg;
    }


}