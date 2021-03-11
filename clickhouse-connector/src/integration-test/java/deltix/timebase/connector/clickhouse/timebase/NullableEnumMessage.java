package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class NullableEnumMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableEnumMessage.class.getName();

    private TestEnum enumField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.ENUM
    )
    public TestEnum getEnumField() {
        return enumField;
    }

    public void setEnumField(TestEnum enumField) {
        this.enumField = enumField;
    }
}
