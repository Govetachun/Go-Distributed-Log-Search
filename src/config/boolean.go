package config

// BooleanFieldConfig represents a boolean field configuration
// Equivalent to BooleanFieldConfig struct in Rust
type BooleanFieldConfig struct {
	Stored      bool `json:"stored" yaml:"stored"`
	Fast        bool `json:"fast" yaml:"fast"`
	Indexed     bool `json:"indexed" yaml:"indexed"`
	ParseString bool `json:"parse_string" yaml:"parse_string"`
}

// NewBooleanFieldConfig creates a new BooleanFieldConfig with defaults
func NewBooleanFieldConfig() *BooleanFieldConfig {
	return &BooleanFieldConfig{
		Stored:      defaultTrue(),
		Fast:        false,
		Indexed:     defaultTrue(),
		ParseString: defaultTrue(),
	}
}

// IsIndexed implements FieldType interface
func (b BooleanFieldConfig) IsIndexed() bool {
	return b.Indexed
}

// ToNumericOptions converts BooleanFieldConfig to NumericOptions equivalent
// Equivalent to From<BooleanFieldConfig> for NumericOptions in Rust
func (b BooleanFieldConfig) ToNumericOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if b.Stored {
		options["stored"] = true
	}
	if b.Indexed {
		options["indexed"] = true
	}
	if b.Fast {
		options["fast"] = true
	}
	return options
}
