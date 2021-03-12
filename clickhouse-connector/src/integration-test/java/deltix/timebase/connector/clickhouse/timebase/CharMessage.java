package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class CharMessage extends InstrumentMessage {
    public static final String CLASS_NAME = CharMessage.class.getName();

    private char charField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.CHAR,
            isNullable = false
    )
    public char getCharField() {
        return charField;
    }

    public void setCharField(char charField) {
        this.charField = charField;
    }
}
