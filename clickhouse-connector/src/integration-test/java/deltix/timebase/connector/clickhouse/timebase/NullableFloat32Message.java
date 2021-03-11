package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class NullableFloat32Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableFloat32Message.class.getName();

    private float nullableFloat32Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_FIXED_FLOAT,
            dataType = SchemaDataType.FLOAT
    )
    public float getNullableFloat32Field() {
        return nullableFloat32Field;
    }

    public void setNullableFloat32Field(float nullableFloat32Field) {
        this.nullableFloat32Field = nullableFloat32Field;
    }
}
