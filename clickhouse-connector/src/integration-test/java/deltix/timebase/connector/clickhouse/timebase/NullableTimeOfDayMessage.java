package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableTimeOfDayMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableTimeOfDayMessage.class.getName();

    private int timeOfDayField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.TIME_OF_DAY
    )
    public int getTimeOfDayField() {
        return timeOfDayField;
    }

    public void setTimeOfDayField(int timeOfDayField) {
        this.timeOfDayField = timeOfDayField;
    }
}
