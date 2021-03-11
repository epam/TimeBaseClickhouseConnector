package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class EnumMessage extends InstrumentMessage {
    public static final String CLASS_NAME = EnumMessage.class.getName();

    private TestEnum enumField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.ENUM,
            isNullable = false
    )
    public TestEnum getEnumField() {
        return enumField;
    }

    public void setEnumField(TestEnum enumField) {
        this.enumField = enumField;
    }
}
