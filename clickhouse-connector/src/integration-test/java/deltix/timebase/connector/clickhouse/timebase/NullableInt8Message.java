package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableInt8Message extends InstrumentMessage {
    public static final String CLASS_NAME = NullableInt8Message.class.getName();

    private byte nullableInt8Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT8,
            dataType = SchemaDataType.INTEGER
    )
    public byte getNullableInt8Field() {
        return nullableInt8Field;
    }

    public void setNullableInt8Field(byte nullableInt8Field) {
        this.nullableInt8Field = nullableInt8Field;
    }
}
