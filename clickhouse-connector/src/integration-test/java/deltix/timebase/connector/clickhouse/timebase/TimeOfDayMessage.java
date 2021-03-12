package deltix.timebase.connector.clickhouse.timebase;

import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class TimeOfDayMessage extends InstrumentMessage {
    public static final String CLASS_NAME = TimeOfDayMessage.class.getName();

    private int timeOfDayField;

    @SchemaElement
    @SchemaType(
            dataType = SchemaDataType.TIME_OF_DAY,
            isNullable = false
    )
    public int getTimeOfDayField() {
        return timeOfDayField;
    }

    public void setTimeOfDayField(int timeOfDayField) {
        this.timeOfDayField = timeOfDayField;
    }
}
