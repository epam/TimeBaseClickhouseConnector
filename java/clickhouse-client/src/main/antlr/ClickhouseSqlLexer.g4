lexer grammar ClickhouseSqlLexer;

NESTED_COLUMN_TYPE:
'Nested'
;

NULLABLE_COLUMN_TYPE:
'Nullable'
;

TUPLE_COLUMN_TYPE:
'Tuple'
;

ARRAY_COLUMN_TYPE:
'Array'
;

ENUM16_COLUMN_TYPE:
'Enum16'
;

ENUM8_COLUMN_TYPE:
'Enum8'
;

FIXED_STRING_COLUMN_TYPE:
'FixedString'
;

DECIMAL_COLUMN_TYPE:
'Decimal32' |
'Decimal64' |
'Decimal128'
;

DECIMAL_RAW_COLUMN_TYPE:
'Decimal'
;

DATE_TIME_64_COLUMN_TYPE:
'DateTime64'
;

SIMPLE_COLUMN_TYPE:
'UInt8' |
'UInt16' |
'UInt32' |
'UInt64' |
'Int8' |
'Int16' |
'Int32' |
'Int64' |
'Float32' |
'Float64' |
'String' |
'Date' |
'DateTime'
;

ENUM_ITEM:                           [a-zA-Z_][0-9a-zA-Z_]*;
COLUMN_NAME:                         [a-zA-Z_][0-9a-zA-Z_]*;

DECIMAL_LITERAL:                     DEC_DIGIT+;

WS:                                  [ \t\r\n]+ -> skip;

DOT:                                 '.';
LR_BRACKET:                          '(';
RR_BRACKET:                          ')';
COMMA:                               ',';
SINGLE_QUOTE_SYMB:                   '\'';
DOUBLE_QUOTE_SYMB:                   '"';
EQUALS:                              '=';


fragment DEC_DIGIT:                  [0-9];
