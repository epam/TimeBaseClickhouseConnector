package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;
import deltix.util.collections.generated.ByteArrayList;


public class NullableBinaryMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableBinaryMessage.class.getName();

    private ByteArrayList binaryField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.BINARY
    )
    public ByteArrayList getBinaryField() {
        return binaryField;
    }

    public void setBinaryField(ByteArrayList binaryField) {
        this.binaryField = binaryField;
    }
}
