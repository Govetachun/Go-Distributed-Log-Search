package config

// IndexRecordOption represents index record options
// Equivalent to IndexRecordOption in Rust
type IndexRecordOption string

const (
	IndexRecordOptionBasic IndexRecordOption = "basic"
	IndexRecordOptionFreq  IndexRecordOption = "freq"
	IndexRecordOptionPos   IndexRecordOption = "pos"
)

// IndexedTextFieldConfig represents indexed text field configuration
type IndexedTextFieldConfig struct {
	Record     IndexRecordOption  `json:"record" yaml:"record"`
	Fieldnorms bool               `json:"fieldnorms" yaml:"fieldnorms"`
	Tokenizer  FieldTokenizerType `json:"tokenizer" yaml:"tokenizer"`
}

// NewIndexedTextFieldConfig creates a new IndexedTextFieldConfig with defaults
func NewIndexedTextFieldConfig() *IndexedTextFieldConfig {
	return &IndexedTextFieldConfig{
		Record:     IndexRecordOptionBasic,
		Fieldnorms: true,
		Tokenizer:  FieldTokenizerTypeDefault,
	}
}

// IndexedTextFieldType represents indexed text field type
type IndexedTextFieldType interface {
	IsIndexed() bool
}

// IndexedTextFieldTypeFalse represents false indexed text field type
type IndexedTextFieldTypeFalse struct{}

func (i IndexedTextFieldTypeFalse) IsIndexed() bool { return false }

// IndexedTextFieldTypeTrue represents true indexed text field type
type IndexedTextFieldTypeTrue struct{}

func (i IndexedTextFieldTypeTrue) IsIndexed() bool { return true }

// IndexedTextFieldTypeIndexed represents indexed indexed text field type
type IndexedTextFieldTypeIndexed struct {
	Config IndexedTextFieldConfig
}

func (i IndexedTextFieldTypeIndexed) IsIndexed() bool { return true }

// TextFieldConfig represents a text field configuration
type TextFieldConfig struct {
	Stored  FastFieldNormalizerType `json:"stored" yaml:"stored"`
	Fast    FastFieldNormalizerType `json:"fast" yaml:"fast"`
	Indexed IndexedTextFieldType    `json:"indexed" yaml:"indexed"`
}

// NewTextFieldConfig creates a new TextFieldConfig with defaults
func NewTextFieldConfig() *TextFieldConfig {
	return &TextFieldConfig{
		Stored:  FastFieldNormalizerTypeFalse,
		Fast:    FastFieldNormalizerTypeFalse,
		Indexed: &IndexedTextFieldTypeTrue{},
	}
}

// IsIndexed implements FieldType interface
func (t TextFieldConfig) IsIndexed() bool {
	return t.Indexed.IsIndexed()
}

// ToTextOptions converts TextFieldConfig to TextOptions equivalent
func (t TextFieldConfig) ToTextOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if t.Stored == FastFieldNormalizerTypeTrue {
		options["stored"] = true
	}
	if fast := t.Fast.From(); fast != nil {
		options["fast"] = *fast
	}

	// Handle indexed options
	if indexed, ok := t.Indexed.(*IndexedTextFieldTypeIndexed); ok {
		indexedOptions := make(map[string]interface{})
		indexedOptions["record"] = string(indexed.Config.Record)
		indexedOptions["fieldnorms"] = indexed.Config.Fieldnorms
		indexedOptions["tokenizer"] = indexed.Config.Tokenizer.From()
		options["indexed"] = indexedOptions
	} else if _, ok := t.Indexed.(*IndexedTextFieldTypeTrue); ok {
		options["indexed"] = true
	}

	return options
}
