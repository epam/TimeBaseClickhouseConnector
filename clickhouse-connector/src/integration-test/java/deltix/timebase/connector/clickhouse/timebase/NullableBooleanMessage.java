package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.*;

public class NullableBooleanMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableBooleanMessage.class.getName();

    private byte nullableBooleanField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.BOOLEAN
    )
    public boolean isNullableBooleanField() {
        return nullableBooleanField == 1;
    }

    public void setNullableBooleanField(boolean nullableBooleanField) {
        this.nullableBooleanField = (byte)(nullableBooleanField ? 1 : 0);
    }

    public boolean hasNullableBooleanField() {
        return nullableBooleanField != TypeConstants.BOOLEAN_NULL;
    }

    public void nullifyNullableBooleanField() {
        this.nullableBooleanField = TypeConstants.BOOLEAN_NULL;
    }
}
