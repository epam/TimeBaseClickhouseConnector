package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableStringMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableStringMessage.class.getName();

    private String nullableStringField;

    @SchemaElement
    public String getNullableStringField() {
        return nullableStringField;
    }

    public void setNullableStringField(String nullableStringField) {
        this.nullableStringField = nullableStringField;
    }
}
