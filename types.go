package sophia

type FieldType string

const (
	FieldType_UInt8     FieldType = "u8"
	FieldType_UInt16    FieldType = "u16"
	FieldType_UInt32    FieldType = "u32"
	FieldType_UInt64    FieldType = "u64"
	FieldType_UInt8Rev  FieldType = "u8rev"
	FieldType_UInt16Rev FieldType = "u16rev"
	FieldType_UInt32Rev FieldType = "u32rev"
	FieldType_UInt64Rev FieldType = "u64rev"
	FieldType_Int64     FieldType = "i64"
	FieldType_String    FieldType = "string"
)
