package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableTimestampMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableTimestampMessage.class.getName();

    private long timestampField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.TIMESTAMP
    )
    public long getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(long timestampField) {
        this.timestampField = timestampField;
    }
}
