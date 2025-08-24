package config

// IndexedDynamicObjectFieldConfig represents indexed dynamic object field configuration
// Equivalent to IndexedDynamicObjectFieldConfig struct in Rust
type IndexedDynamicObjectFieldConfig struct {
	Record    IndexRecordOption  `json:"record" yaml:"record"`
	Tokenizer FieldTokenizerType `json:"tokenizer" yaml:"tokenizer"`
}

// NewIndexedDynamicObjectFieldConfig creates a new IndexedDynamicObjectFieldConfig with defaults
func NewIndexedDynamicObjectFieldConfig() *IndexedDynamicObjectFieldConfig {
	return &IndexedDynamicObjectFieldConfig{
		Record:    IndexRecordOptionBasic,
		Tokenizer: FieldTokenizerTypeRaw,
	}
}

// NewIndexedDynamicObjectFieldType creates a new indexed dynamic object field type
func NewIndexedDynamicObjectFieldType() IndexedDynamicObjectFieldType {
	return &IndexedDynamicObjectFieldTypeIndexed{
		Config: *NewIndexedDynamicObjectFieldConfig(),
	}
}

// IndexedDynamicObjectFieldType represents indexed dynamic object field type
// Equivalent to IndexedDynamicObjectFieldType enum in Rust
type IndexedDynamicObjectFieldType interface {
	IsIndexed() bool
}

// IndexedDynamicObjectFieldTypeFalse represents false indexed dynamic object field type
type IndexedDynamicObjectFieldTypeFalse struct{}

func (i IndexedDynamicObjectFieldTypeFalse) IsIndexed() bool { return false }

// IndexedDynamicObjectFieldTypeTrue represents true indexed dynamic object field type
type IndexedDynamicObjectFieldTypeTrue struct{}

func (i IndexedDynamicObjectFieldTypeTrue) IsIndexed() bool { return true }

// IndexedDynamicObjectFieldTypeIndexed represents indexed indexed dynamic object field type
type IndexedDynamicObjectFieldTypeIndexed struct {
	Config IndexedDynamicObjectFieldConfig
}

func (i IndexedDynamicObjectFieldTypeIndexed) IsIndexed() bool { return true }

// DynamicObjectFieldConfig represents a dynamic object field configuration
// Equivalent to DynamicObjectFieldConfig struct in Rust
type DynamicObjectFieldConfig struct {
	Stored     bool                          `json:"stored" yaml:"stored"`
	Fast       FastFieldNormalizerType       `json:"fast" yaml:"fast"`
	Indexed    IndexedDynamicObjectFieldType `json:"indexed" yaml:"indexed"`
	ExpandDots bool                          `json:"expand_dots" yaml:"expand_dots"`
}

// NewDynamicObjectFieldConfig creates a new DynamicObjectFieldConfig with defaults
func NewDynamicObjectFieldConfig() *DynamicObjectFieldConfig {
	return &DynamicObjectFieldConfig{
		Stored:     defaultTrue(),
		Fast:       FastFieldNormalizerTypeTrue,
		Indexed:    &IndexedDynamicObjectFieldTypeTrue{},
		ExpandDots: defaultTrue(),
	}
}

// IsIndexed implements FieldType interface
func (d DynamicObjectFieldConfig) IsIndexed() bool {
	return d.Indexed.IsIndexed()
}

// ToJsonObjectOptions converts DynamicObjectFieldConfig to JsonObjectOptions equivalent
// Equivalent to From<DynamicObjectFieldConfig> for JsonObjectOptions in Rust
func (d DynamicObjectFieldConfig) ToJsonObjectOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if d.Stored {
		options["stored"] = true
	}
	if fast := d.Fast.From(); fast != nil {
		options["fast"] = *fast
	}

	// Handle indexed options
	if indexed, ok := d.Indexed.(*IndexedDynamicObjectFieldTypeIndexed); ok {
		indexedOptions := make(map[string]interface{})
		indexedOptions["record"] = string(indexed.Config.Record)
		indexedOptions["tokenizer"] = indexed.Config.Tokenizer.From()
		options["indexed"] = indexedOptions
	} else if _, ok := d.Indexed.(*IndexedDynamicObjectFieldTypeTrue); ok {
		options["indexed"] = true
	}

	if d.ExpandDots {
		options["expand_dots"] = true
	}

	return options
}
