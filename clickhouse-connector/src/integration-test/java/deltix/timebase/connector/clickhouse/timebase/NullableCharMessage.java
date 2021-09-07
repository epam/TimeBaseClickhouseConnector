package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.timebase.messages.*;

public class NullableCharMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableCharMessage.class.getName();

    private char charField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.CHAR
    )
    public char getCharField() {
        return charField;
    }

    public void setCharField(char charField) {
        this.charField = charField;
    }
}
