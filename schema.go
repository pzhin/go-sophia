package sophia

import "fmt"

type Schema struct {
	// name -> type
	keys      map[string]FieldType
	keysNames []string
	// name -> type
	values      map[string]FieldType
	valuesNames []string
}

func (s *Schema) AddKey(name string, typ FieldType) error {
	if s.keys == nil {
		s.keys = make(map[string]FieldType)
	}
	if _, ok := s.keys[name]; ok {
		return fmt.Errorf("duplicate key, '%v' has been already defined", name)
	}
	s.keysNames = append(s.keysNames, name)
	s.keys[name] = typ
	return nil
}

func (s *Schema) AddValue(name string, typ FieldType) error {
	if s.values == nil {
		s.values = make(map[string]FieldType)
	}
	if _, ok := s.values[name]; ok {
		return fmt.Errorf("duplicate value, '%v' is already defined", name)
	}
	s.valuesNames = append(s.valuesNames, name)
	s.values[name] = typ
	return nil
}

func defaultSchema() *Schema {
	schema := &Schema{}
	schema.AddKey("key", FieldTypeString)
	schema.AddKey("value", FieldTypeString)
	return schema
}
