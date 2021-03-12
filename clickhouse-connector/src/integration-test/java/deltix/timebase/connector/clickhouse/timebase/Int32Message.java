package deltix.timebase.connector.clickhouse.timebase;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class Int32Message extends InstrumentMessage {
    public static final String CLASS_NAME = Int32Message.class.getName();

    private int int32Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT32,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public int getInt32Field() {
        return int32Field;
    }

    public void setInt32Field(int int32Field) {
        this.int32Field = int32Field;
    }
}
