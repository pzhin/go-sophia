package sophia

type FieldType int

const (
	FieldType_UInt8 FieldType = iota
	FieldType_UInt16
	FieldType_UInt32
	FieldType_UInt64
	FieldType_UInt8Rev
	FieldType_UInt16Rev
	FieldType_UInt32Rev
	FieldType_UInt64Rev
	FieldType_Int64
	FieldType_String
)

var fieldTypeNames = map[FieldType]string{
	FieldType_UInt8:     "u8",
	FieldType_UInt16:    "u16",
	FieldType_UInt32:    "u32",
	FieldType_UInt64:    "u64",
	FieldType_UInt8Rev:  "u8rev",
	FieldType_UInt16Rev: "u16rev",
	FieldType_UInt32Rev: "u32rev",
	FieldType_UInt64Rev: "u64rev",
	FieldType_Int64:     "i64",
	FieldType_String:    "string",
}

func (t *FieldType) String() string {
	return fieldTypeNames[*t]
}
