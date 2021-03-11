package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class NullableFloat64Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableFloat64Message.class.getName();

    private double nullableFloat64Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_FIXED_DOUBLE,
            dataType = SchemaDataType.FLOAT
    )
    public double getNullableFloat64Field() {
        return nullableFloat64Field;
    }

    public void setNullableFloat64Field(double nullableFloat64Field) {
        this.nullableFloat64Field = nullableFloat64Field;
    }
}
