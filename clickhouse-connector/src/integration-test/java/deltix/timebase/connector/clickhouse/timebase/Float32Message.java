package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import com.epam.deltix.timebase.messages.InstrumentMessage;
import com.epam.deltix.timebase.messages.SchemaDataType;
import com.epam.deltix.timebase.messages.SchemaElement;
import com.epam.deltix.timebase.messages.SchemaType;


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
