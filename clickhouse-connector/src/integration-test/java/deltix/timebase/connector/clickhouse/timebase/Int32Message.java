package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import com.epam.deltix.timebase.messages.InstrumentMessage;
import com.epam.deltix.timebase.messages.SchemaDataType;
import com.epam.deltix.timebase.messages.SchemaElement;
import com.epam.deltix.timebase.messages.SchemaType;


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
