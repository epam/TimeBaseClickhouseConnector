package deltix.timebase.connector.clickhouse.util;

import deltix.clickhouse.schema.types.Decimal128DataType;
import deltix.clickhouse.schema.types.DecimalDataType;
import deltix.clickhouse.util.SelectQueryHelper;
import deltix.timebase.connector.clickhouse.algos.SchemaProcessor;
import ru.yandex.clickhouse.domain.ClickHouseDataType;

import java.math.BigDecimal;

public class ClickhouseUtil {
    public static final BigDecimal DECIMAL_128_MAX_VALUE = getDecimalMaxValue(new Decimal128DataType(SchemaProcessor.DEFAULT_DECIMAL_SCALE));
    public static final BigDecimal DECIMAL_128_MIN_VALUE = getDecimalMinValue(new Decimal128DataType(SchemaProcessor.DEFAULT_DECIMAL_SCALE));


    public static BigDecimal getDecimalMinValue(deltix.clickhouse.schema.types.DataType dataType) {
        return getDecimalMaxValue(dataType).negate();
    }

    public static BigDecimal getDecimalMaxValue(deltix.clickhouse.schema.types.DataType dataType) {
        final deltix.clickhouse.schema.types.DataType strippedType = SelectQueryHelper.stripNullable(dataType);
        final int precision, scale;
        switch (strippedType.getType()) {
            case DECIMAL:
                DecimalDataType decimalDataType = (DecimalDataType) strippedType;
                precision = decimalDataType.getP();
                scale = decimalDataType.getS();
                break;
            case DECIMAL32:
                precision = ClickHouseDataType.Decimal32.getDefaultPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;
            case DECIMAL64:
                precision = ClickHouseDataType.Decimal64.getDefaultPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;
            case DECIMAL128:
                precision = ClickHouseDataType.Decimal128.getDefaultPrecision();
                scale = SelectQueryHelper.calculateDecimalScaleByType(strippedType);
                break;

            default:
                throw new UnsupportedOperationException();
        }

        StringBuilder decimalValueAsStr = new StringBuilder();
        int digitsExcludeFraction = precision - scale;
        for (int i = 0; i < digitsExcludeFraction; i++) {
            decimalValueAsStr.append('9');
        }

        for (int i = 0; i < scale; i++) {
            if (i == 0)
                decimalValueAsStr.append('.');

            decimalValueAsStr.append('9');
        }

        return new BigDecimal(decimalValueAsStr.toString());
    }
}
