package deltix.timebase.connector.clickhouse.timebase;

import com.epam.deltix.qsrv.hf.pub.md.FloatDataType;
import deltix.timebase.messages.InstrumentMessage;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;


public class NullableDecimalMessage extends InstrumentMessage {
    public static final String CLASS_NAME = NullableDecimalMessage.class.getName();

    private double decimalField;

    @SchemaElement
    @SchemaType(
            encoding = FloatDataType.ENCODING_SCALE_AUTO,
            dataType = SchemaDataType.FLOAT
    )
    public double getDecimalField() {
        return decimalField;
    }

    public void setDecimalField(double decimalField) {
        this.decimalField = decimalField;
    }
}
