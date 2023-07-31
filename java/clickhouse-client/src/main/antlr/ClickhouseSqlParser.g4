parser grammar ClickhouseSqlParser;

options { tokenVocab=ClickhouseSqlLexer; }

root: columnTypeLiteral EOF;

columnDefinitionCollectionLiteral :
    LR_BRACKET columnDefinitionLiteral (COMMA columnTypeLiteral)* RR_BRACKET;

columnDefinitionLiteral :
    COLUMN_NAME columnTypeLiteral;

columnTypeLiteral :
    primitiveColumnTypeLiteral |
    arrayColumnTypeLiteral |
    tupleColumnTypeLiteral |
    nullableColumnTypeLiteral |
    nestedColumnTypeLiteral;

primitiveColumnTypeLiteral:
    simpleColumnTypeLiteral |
    fixedStringColumnTypeLiteral |
    enumColumnTypeLiteral |
    decimalColumnTypeLiteral |
    decimalRawColumnTypeLiteral |
    dateTime64ColumnTypeLiteral;

nestedColumnTypeLiteral:
    NESTED_COLUMN_TYPE columnDefinitionCollectionLiteral;

nullableColumnTypeLiteral:
    NULLABLE_COLUMN_TYPE LR_BRACKET primitiveColumnTypeLiteral RR_BRACKET;

tupleColumnTypeLiteral:
    TUPLE_COLUMN_TYPE LR_BRACKET columnTypeLiteral (COMMA columnTypeLiteral)* RR_BRACKET;

arrayColumnTypeLiteral:
    ARRAY_COLUMN_TYPE LR_BRACKET columnTypeLiteral RR_BRACKET;

decimalColumnTypeLiteral:
    DECIMAL_COLUMN_TYPE LR_BRACKET decimalSLiteral RR_BRACKET;

decimalRawColumnTypeLiteral:
    DECIMAL_RAW_COLUMN_TYPE LR_BRACKET decimalPLiteral COMMA decimalSLiteral RR_BRACKET;

decimalPLiteral:
    DECIMAL_LITERAL;

decimalSLiteral:
    DECIMAL_LITERAL;

fixedStringColumnTypeLiteral:
    FIXED_STRING_COLUMN_TYPE LR_BRACKET fixedStringColumnLengthLiteral RR_BRACKET;

fixedStringColumnLengthLiteral:
    DECIMAL_LITERAL;

dateTime64ColumnTypeLiteral:
    DATE_TIME_64_COLUMN_TYPE (LR_BRACKET dateTime64PrecisionLiteral RR_BRACKET)?;

dateTime64PrecisionLiteral:
    DECIMAL_LITERAL;

simpleColumnTypeLiteral:
    SIMPLE_COLUMN_TYPE;

enumColumnTypeLiteral:
    enum8ColumnTypeLiteral | enum16ColumnTypeLiteral;

enum16ColumnTypeLiteral:
    ENUM16_COLUMN_TYPE LR_BRACKET enumItemLiteral (COMMA enumItemLiteral)* RR_BRACKET;

enum8ColumnTypeLiteral:
    ENUM8_COLUMN_TYPE LR_BRACKET enumItemLiteral (COMMA enumItemLiteral)* RR_BRACKET;

enumItemLiteral:
    SINGLE_QUOTE_SYMB enumItemNameLiteral SINGLE_QUOTE_SYMB EQUALS enumItemNumberLiteral;

enumItemNameLiteral:
    ENUM_ITEM?;

enumItemNumberLiteral:
    DECIMAL_LITERAL;

