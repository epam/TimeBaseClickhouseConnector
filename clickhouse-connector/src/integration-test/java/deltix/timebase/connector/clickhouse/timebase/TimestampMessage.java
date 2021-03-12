package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class TimestampMessage extends InstrumentMessage {
    public static final String CLASS_NAME = TimestampMessage.class.getName();

    private long timestampField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.TIMESTAMP,
            isNullable = false
    )
    public long getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(long timestampField) {
        this.timestampField = timestampField;
    }
}
