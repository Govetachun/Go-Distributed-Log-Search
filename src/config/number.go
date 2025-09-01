package config

// NumberFieldType represents numeric types
// Equivalent to NumberFieldType enum in Rust
type NumberFieldType string

const (
	NumberFieldTypeU64 NumberFieldType = "u64"
	NumberFieldTypeI64 NumberFieldType = "i64"
	NumberFieldTypeF64 NumberFieldType = "f64"
)

// NumberFieldConfig represents a number field configuration
type NumberFieldConfig struct {
	Type        NumberFieldType `json:"type" yaml:"type"`
	Stored      bool            `json:"stored" yaml:"stored"`
	Fast        bool            `json:"fast" yaml:"fast"`
	Indexed     bool            `json:"indexed" yaml:"indexed"`
	ParseString bool            `json:"parse_string" yaml:"parse_string"`
}

// NewNumberFieldConfig creates a new NumberFieldConfig with defaults
func NewNumberFieldConfig(numberType NumberFieldType) *NumberFieldConfig {
	return &NumberFieldConfig{
		Type:        numberType,
		Stored:      defaultTrue(),
		Fast:        false,
		Indexed:     defaultTrue(),
		ParseString: defaultTrue(),
	}
}

// IsIndexed implements FieldType interface
func (n NumberFieldConfig) IsIndexed() bool {
	return n.Indexed
}

// ToNumericOptions converts NumberFieldConfig to NumericOptions equivalent
func (n NumberFieldConfig) ToNumericOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if n.Stored {
		options["stored"] = true
	}
	if n.Indexed {
		options["indexed"] = true
	}
	if n.Fast {
		options["fast"] = true
	}
	return options
}
