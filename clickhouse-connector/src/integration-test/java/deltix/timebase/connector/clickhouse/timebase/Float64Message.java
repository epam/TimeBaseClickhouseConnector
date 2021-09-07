package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class Float64Message extends InstrumentMessage {
    public static final String CLASS_NAME = Float64Message.class.getName();

    private double float64Field;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_FIXED_DOUBLE,
            dataType = SchemaDataType.FLOAT,
            isNullable = false
    )
    public double getFloat64Field() {
        return float64Field;
    }

    public void setFloat64Field(double float64Field) {
        this.float64Field = float64Field;
    }
}
