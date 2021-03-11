package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class BooleanMessage extends InstrumentMessage {
    public static final String CLASS_NAME = BooleanMessage.class.getName();

    private boolean booleanField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.BOOLEAN,
            isNullable = false
    )
    public boolean isBooleanField() {
        return booleanField;
    }

    public void setBooleanField(boolean booleanField) {
        this.booleanField = booleanField;
    }
}
