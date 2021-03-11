package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;
import deltix.util.collections.generated.ByteArrayList;

public class BinaryMessage extends InstrumentMessage {
    public static final String CLASS_NAME = BinaryMessage.class.getName();

    private ByteArrayList binaryField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.BINARY,
            isNullable = false
    )
    public ByteArrayList getBinaryField() {
        return binaryField;
    }

    public void setBinaryField(ByteArrayList binaryField) {
        this.binaryField = binaryField;
    }
}
