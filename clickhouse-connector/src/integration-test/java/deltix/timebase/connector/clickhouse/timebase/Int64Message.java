package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class Int64Message extends InstrumentMessage {
    public static final String CLASS_NAME = Int64Message.class.getName();

    private long int64Field;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER,
            isNullable = false
    )
    public long getInt64Field() {
        return int64Field;
    }

    public void setInt64Field(long int64Field) {
        this.int64Field = int64Field;
    }
}
