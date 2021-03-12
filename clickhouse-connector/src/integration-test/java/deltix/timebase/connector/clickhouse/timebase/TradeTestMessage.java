package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.*;

/**
 * Basic information about a market trade.
 */
@OldElementName("deltix.qsrv.hf.pub.TradeMessage")
@SchemaElement(
        name = "deltix.timebase.messages.messages.TradeMessage",
        title = "Trade Message"
)
public class TradeTestMessage extends InstrumentMessage {
    public static final String CLASS_NAME = TradeTestMessage.class.getName();

    /**
     * Exchange code compressed to long using ALPHANUMERIC(10) encoding
     */
    protected long exchangeId = TypeConstants.EXCHANGE_NULL;

    /**
     * The trade price.
     */
    protected double price = TypeConstants.IEEE64_NULL;

    /**
     * The trade size.
     */
    protected double size = TypeConstants.IEEE64_NULL;

    /**
     * Market specific trade condition.
     */
    protected CharSequence condition = null;

    /**
     * Net change from previous days closing price vs. last traded price.
     */
    protected double netPriceChange = TypeConstants.IEEE64_NULL;

    /**
     * Market specific identifier of the given event in a sequence of market events.
     */
    protected long sequenceNumber = TypeConstants.INT64_NULL;

    /**
     * Exchange code compressed to long using ALPHANUMERIC(10) encoding
     * @return Exchange Id
     */
    @SchemaType(
            encoding = "ALPHANUMERIC(10)",
            dataType = SchemaDataType.VARCHAR
    )
    @SchemaElement
    @OldElementName("exchangeCode")
    public long getExchangeId() {
        return exchangeId;
    }

    /**
     * Exchange code compressed to long using ALPHANUMERIC(10) encoding
     * @param value - Exchange Id
     */
    public void setExchangeId(long value) {
        this.exchangeId = value;
    }

    /**
     * Exchange code compressed to long using ALPHANUMERIC(10) encoding
     * @return true if Exchange Id is not null
     */
    public boolean hasExchangeId() {
        return exchangeId != deltix.timebase.messages.TypeConstants.EXCHANGE_NULL;
    }

    /**
     * Exchange code compressed to long using ALPHANUMERIC(10) encoding
     */
    public void nullifyExchangeId() {
        this.exchangeId = deltix.timebase.messages.TypeConstants.EXCHANGE_NULL;
    }

    /**
     * The trade price.
     * @return Price
     */
    @SchemaType(
            encoding = "DECIMAL(8)",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public double getPrice() {
        return price;
    }

    /**
     * The trade price.
     * @param value - Price
     */
    public void setPrice(double value) {
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
        this.price = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
    }

    /**
     * The trade size.
     * @return Size
     */
    @SchemaType(
            encoding = "DECIMAL(8)",
            dataType = SchemaDataType.FLOAT
    )
    @SchemaElement
    public double getSize() {
        return size;
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
        this.size = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
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
     * Net change from previous days closing price vs. last traded price.
     * @return Net Price Change
     */
    @SchemaElement
    public double getNetPriceChange() {
        return netPriceChange;
    }

    /**
     * Net change from previous days closing price vs. last traded price.
     * @param value - Net Price Change
     */
    public void setNetPriceChange(double value) {
        this.netPriceChange = value;
    }

    /**
     * Net change from previous days closing price vs. last traded price.
     * @return true if Net Price Change is not null
     */
    public boolean hasNetPriceChange() {
        return !Double.isNaN(netPriceChange);
    }

    /**
     * Net change from previous days closing price vs. last traded price.
     */
    public void nullifyNetPriceChange() {
        this.netPriceChange = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
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
        return sequenceNumber != deltix.timebase.messages.TypeConstants.INT64_NULL;
    }

    /**
     * Market specific identifier of the given event in a sequence of market events.
     */
    public void nullifySequenceNumber() {
        this.sequenceNumber = deltix.timebase.messages.TypeConstants.INT64_NULL;
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
        nullifyExchangeId();
        nullifyPrice();
        nullifySize();
        nullifyCondition();
        nullifyNetPriceChange();
        return this;
    }

    /**
     * Resets all instance properties to their default values
     */
    @Override
    public TradeTestMessage reset() {
        super.reset();
        exchangeId = deltix.timebase.messages.TypeConstants.EXCHANGE_NULL;
        price = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
        size = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
        condition = null;
        netPriceChange = deltix.timebase.messages.TypeConstants.IEEE64_NULL;
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
        if (hasExchangeId() != other.hasExchangeId()) return false;
        if (hasExchangeId() && getExchangeId() != other.getExchangeId()) return false;
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
        if (hasNetPriceChange() != other.hasNetPriceChange()) return false;
        if (hasNetPriceChange() && getNetPriceChange() != other.getNetPriceChange()) return false;
        return true;
    }

    /**
     * Returns a hash code value for the object. This method is * supported for the benefit of hash tables such as those provided by.
     */
    @Override
    public int hashCode() {
        int hash = super.hashCode();
        if (hasExchangeId()) {
            hash = hash * 31 + ((int)(getExchangeId() ^ (getExchangeId() >>> 32)));
        }
        if (hasPrice()) {
            hash = hash * 31 + ((int)(Double.doubleToLongBits(getPrice()) ^ (Double.doubleToLongBits(getPrice()) >>> 32)));
        }
        if (hasSize()) {
            hash = hash * 31 + ((int)(Double.doubleToLongBits(getSize()) ^ (Double.doubleToLongBits(getSize()) >>> 32)));
        }
        if (hasCondition()) {
            hash = hash * 31 + getCondition().hashCode();
        }
        if (hasNetPriceChange()) {
            hash = hash * 31 + ((int)(Double.doubleToLongBits(getNetPriceChange()) ^ (Double.doubleToLongBits(getNetPriceChange()) >>> 32)));
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
            if (t.hasExchangeId()) {
                setExchangeId(t.getExchangeId());
            } else {
                nullifyExchangeId();
            }
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
            if (t.hasNetPriceChange()) {
                setNetPriceChange(t.getNetPriceChange());
            } else {
                nullifyNetPriceChange();
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
        if (hasExchangeId()) {
            str.append(", \"exchangeId\": ").append(getExchangeId());
        }
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
        if (hasNetPriceChange()) {
            str.append(", \"netPriceChange\": ").append(getNetPriceChange());
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
