package config

// StaticObjectFieldConfig represents a static object field configuration
// Equivalent to StaticObjectFieldConfig struct in Rust
type StaticObjectFieldConfig struct {
	Fields FieldConfigs `json:"fields" yaml:"fields"`
}

// NewStaticObjectFieldConfig creates a new StaticObjectFieldConfig
func NewStaticObjectFieldConfig(fields FieldConfigs) *StaticObjectFieldConfig {
	return &StaticObjectFieldConfig{
		Fields: fields,
	}
}

// IsIndexed implements FieldType interface
func (s StaticObjectFieldConfig) IsIndexed() bool {
	return false
}
