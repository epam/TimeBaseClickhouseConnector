package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class StringMessage extends InstrumentMessage {
    public static final String CLASS_NAME = StringMessage.class.getName();

    private String stringField;

    @SchemaElement
    @SchemaType(isNullable = false)
    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }
}
