package deltix.timebase.connector.clickhouse.timebase;


import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.RecordInfo;

@deltix.timebase.messages.SchemaElement(name = "deltix.timebase.connector.clickhouse.timebase.ObjectFieldMessage")
public class ObjectFieldMessage extends InstrumentMessage {
    /**
     *
     */
    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class})
    public InstrumentMessage bboField;
    /**
     *
     */
    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class, TradeTestMessage.class})
    public InstrumentMessage marketField;
    /**
     *
     */
    public String stringField;
    /**
     *
     */
    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {TradeTestMessage.class})
    public InstrumentMessage tradeField;

    @java.lang.Override
    public InstrumentMessage copyFrom(RecordInfo source) {
        super.copyFrom(source);
        if (source instanceof ObjectFieldMessage) {
            final ObjectFieldMessage obj = (ObjectFieldMessage) source;
            bboField = obj.bboField;
            marketField = obj.marketField;
            stringField = obj.stringField;
            tradeField = obj.tradeField;
        }
        return this;
    }

    @java.lang.Override
    public String toString() {
        return super.toString() + ", " + "bboField" + ": " + bboField + ", " + "marketField" + ": " + marketField + ", " + "stringField" + ": " + stringField + ", " + "tradeField" + ": " + tradeField;
    }

    @java.lang.Override
    public InstrumentMessage clone() {
        final ObjectFieldMessage msg = new ObjectFieldMessage();
        msg.copyFrom(this);
        return msg;
    }

    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class, TradeTestMessage.class})
    public InstrumentMessage getBboField() {
        return bboField;
    }

    public void setBboField(InstrumentMessage bboField) {
        this.bboField = bboField;
    }

    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class, TradeTestMessage.class})
    public InstrumentMessage getMarketField() {
        return marketField;
    }

    public void setMarketField(InstrumentMessage marketField) {
        this.marketField = marketField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    @deltix.timebase.messages.SchemaType(dataType = deltix.timebase.messages.SchemaDataType.OBJECT, nestedTypes = {BestBidOfferTestMessage.class, TradeTestMessage.class})
    public InstrumentMessage getTradeField() {
        return tradeField;
    }

    public void setTradeField(InstrumentMessage tradeField) {
        this.tradeField = tradeField;
    }
}