package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class Int16Message extends InstrumentMessage {
    public static final String CLASS_NAME = Int16Message.class.getName();

    private short int16Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT16,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public short getInt16Field() {
        return int16Field;
    }

    public void setInt16Field(short int16Field) {
        this.int16Field = int16Field;
    }
}
