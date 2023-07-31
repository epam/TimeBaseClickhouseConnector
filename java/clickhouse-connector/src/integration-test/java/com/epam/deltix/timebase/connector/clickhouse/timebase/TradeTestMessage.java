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


/**
 * Basic information about a market trade.
 */
@OldElementName("deltix.qsrv.hf.pub.TradeMessage")
@SchemaElement()
public class TradeTestMessage extends InstrumentMessage {
    public static final String CLASS_NAME = TradeTestMessage.class.getName();

    /**
     * The trade price.
     */
    @SchemaType(
            encoding = "IEEE32",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public float price = TypeConstants.IEEE32_NULL;

    /**
     * The trade size.
     */
    @SchemaType(
            encoding = "IEEE64",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public double size = TypeConstants.IEEE64_NULL;

    /**
     * Market specific trade condition.
     */
    protected CharSequence condition = null;


    /**
     * Market specific identifier of the given event in a sequence of market events.
     */
    protected long sequenceNumber = TypeConstants.INT64_NULL;

    protected long timestampField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.TIMESTAMP,
            isNullable = false
    )
    public long getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(long timestampField) {
        this.timestampField = timestampField;
    }
    /**
     * The trade price.
     * @return Price
     */
    public Float getPrice() {
        return Float.isNaN(price) ? null : price;
    }

    /**
     * The trade price.
     * @param value - Price
     */
    public void setPrice(float value) {
        this.price = value;
    }

    /**
     * The trade price.
     * @return true if Price is not null
     */
    public boolean hasPrice() {
        return !Double.isNaN(price);
    }

    /**
     * The trade price.
     */
    public void nullifyPrice() {
        this.price = TypeConstants.IEEE32_NULL;
    }

    /**
     * The trade size.
     * @return Size
     */

    public Double getSize() {
        return Double.isNaN(size) ? null : size;
    }

    /**
     * The trade size.
     * @param value - Size
     */
    public void setSize(double value) {
        this.size = value;
    }

    /**
     * The trade size.
     * @return true if Size is not null
     */
    public boolean hasSize() {
        return !Double.isNaN(size);
    }

    /**
     * The trade size.
     */
    public void nullifySize() {
        this.size = TypeConstants.IEEE64_NULL;
    }

    /**
     * Market specific trade condition.
     * @return Condition
     */
    @SchemaType(
            encoding = "UTF8",
            dataType = SchemaDataType.VARCHAR
    )
    @SchemaElement
    public CharSequence getCondition() {
        return condition;
    }

    /**
     * Market specific trade condition.
     * @param value - Condition
     */
    public void setCondition(CharSequence value) {
        this.condition = value;
    }

    /**
     * Market specific trade condition.
     * @return true if Condition is not null
     */
    public boolean hasCondition() {
        return condition != null;
    }

    /**
     * Market specific trade condition.
     */
    public void nullifyCondition() {
        this.condition = null;
    }

    /**
     * Market specific identifier of the given event in a sequence of market events.
     * @return Sequence Number
     */
    @SchemaElement
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Market specific identifier of the given event in a sequence of market events.
     * @param value - Sequence Number
     */
    public void setSequenceNumber(long value) {
        this.sequenceNumber = value;
    }

    /**
     * Market specific identifier of the given event in a sequence of market events.
     * @return true if Sequence Number is not null
     */
    public boolean hasSequenceNumber() {
        return sequenceNumber != TypeConstants.INT64_NULL;
    }

    /**
     * Market specific identifier of the given event in a sequence of market events.
     */
    public void nullifySequenceNumber() {
        this.sequenceNumber = TypeConstants.INT64_NULL;
    }

    /**
     * Creates new instance of this class.
     * @return new instance of this class.
     */
    @Override
    protected TradeTestMessage createInstance() {
        return new TradeTestMessage();
    }

    /**
     * Method nullifies all instance properties
     */
    @Override
    public TradeTestMessage nullify() {
        super.nullify();
        nullifyPrice();
        nullifySize();
        nullifyCondition();
        return this;
    }

    /**
     * Resets all instance properties to their default values
     */
    @Override
    public TradeTestMessage reset() {
        super.reset();
        price = TypeConstants.IEEE32_NULL;
        size = TypeConstants.IEEE64_NULL;
        condition = null;
        return this;
    }

    /**
     * Method copies state to a given instance
     */
    @Override
    public TradeTestMessage clone() {
        TradeTestMessage t = createInstance();
        t.copyFrom(this);
        return t;
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        boolean superEquals = super.equals(obj);
        if (!superEquals) return false;
        if (!(obj instanceof TradeTestMessage)) return false;
        TradeTestMessage other =(TradeTestMessage)obj;
        if (hasPrice() != other.hasPrice()) return false;
        if (hasPrice() && getPrice() != other.getPrice()) return false;
        if (hasSize() != other.hasSize()) return false;
        if (hasSize() && getSize() != other.getSize()) return false;
        if (hasCondition() != other.hasCondition()) return false;
        if (hasCondition()) {
            if (getCondition().length() != other.getCondition().length()) return false; else {
                String s1 = getCondition().toString();
                String s2 = other.getCondition().toString();
                if (!s1.equals(s2)) return false;
            }
        }
        if (hasSequenceNumber() != other.hasSequenceNumber()) return false;
        if (hasSequenceNumber() && getSequenceNumber() != other.getSequenceNumber()) return false;
        return true;
    }

    /**
     * Returns a hash code value for the object. This method is * supported for the benefit of hash tables such as those provided by.
     */
    @Override
    public int hashCode() {
        int hash = super.hashCode();
        if (hasPrice()) {
            hash = hash * 31 + ((int)(Double.doubleToLongBits(getPrice()) ^ (Double.doubleToLongBits(getPrice()) >>> 32)));
        }
        if (hasSize()) {
            hash = hash * 31 + ((int)(Double.doubleToLongBits(getSize()) ^ (Double.doubleToLongBits(getSize()) >>> 32)));
        }
        if (hasCondition()) {
            hash = hash * 31 + getCondition().hashCode();
        }
        return hash;
    }

    /**
     * Method copies state to a given instance
     * @param template class instance that should be used as a copy source
     */
    @Override
    public TradeTestMessage copyFrom(RecordInfo template) {
        super.copyFrom(template);
        if (template instanceof TradeTestMessage) {
            TradeTestMessage t = (TradeTestMessage)template;
            if (t.hasPrice()) {
                setPrice(t.getPrice());
            } else {
                nullifyPrice();
            }
            if (t.hasSize()) {
                setSize(t.getSize());
            } else {
                nullifySize();
            }
            if (t.hasCondition()) {
                if (hasCondition() && getCondition() instanceof StringBuilder) {
                    ((StringBuilder)getCondition()).setLength(0);
                } else {
                    setCondition(new StringBuilder());
                }
                ((StringBuilder)getCondition()).append(t.getCondition());
            } else {
                nullifyCondition();
            }
            if (t.hasSequenceNumber()) {
                setSequenceNumber(t.getSequenceNumber());
            } else {
                nullifySequenceNumber();
            }
        }
        return this;
    }

    /**
     * @return a string representation of this class object.
     */
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        return toString(str).toString();
    }

    /**
     * @return a string representation of this class object.
     */
    @Override
    public StringBuilder toString(StringBuilder str) {
        str.append("{ \"$type\":  \"TradeMessage\"");
        if (hasPrice()) {
            str.append(", \"price\": ").append(getPrice());
        }
        if (hasSize()) {
            str.append(", \"size\": ").append(getSize());
        }
        if (hasCondition()) {
            str.append(", \"condition\": \"").append(getCondition()).append("\"");
        }
        if (hasSequenceNumber()) {
            str.append(", \"aggressorSide\": \"").append(getSequenceNumber()).append("\"");
        }
        if (hasSequenceNumber()) {
            str.append(", \"sequenceNumber\": ").append(getSequenceNumber());
        }
        if (hasSymbol()) {
            str.append(", \"symbol\": \"").append(getSymbol()).append("\"");
        }
        str.append("}");
        return str;
    }
}