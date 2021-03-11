package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class Int8Message extends InstrumentMessage {
    public static final String CLASS_NAME = Int8Message.class.getName();

    private byte int8Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT8,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public byte getInt8Field() {
        return int8Field;
    }

    public void setInt8Field(byte int8Field) {
        this.int8Field = int8Field;
    }
}
