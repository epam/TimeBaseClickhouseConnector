package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class Float32Message extends InstrumentMessage {
    public static final String CLASS_NAME = Float32Message.class.getName();

    private float float32Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_FIXED_FLOAT,
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public float getFloat32Field() {
        return float32Field;
    }

    public void setFloat32Field(float float32Field) {
        this.float32Field = float32Field;
    }
}
