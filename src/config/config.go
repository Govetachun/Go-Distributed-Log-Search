package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// IndexConfig represents the main index configuration
type IndexConfig struct {
	Name   string       `json:"name" yaml:"name"`
	Path   string       `json:"path" yaml:"path"`
	Schema SchemaConfig `json:"schema" yaml:"schema"`
}

// SchemaConfig represents the schema configuration
type SchemaConfig struct {
	Fields FieldConfigs `json:"fields" yaml:"fields"`
}

// FieldConfig represents a field configuration
type FieldConfig struct {
	Name  string    `json:"name" yaml:"name"`
	Array bool      `json:"array" yaml:"array"`
	Type  FieldType `json:"type" yaml:"type"`
}

// FieldType represents different field types
type FieldType interface {
	IsIndexed() bool
	IsStaticObject() bool
}

// TextFieldType represents a text field
type TextFieldType struct {
	Stored    bool                `json:"stored" yaml:"stored"`
	Indexed   bool                `json:"indexed" yaml:"indexed"`
	Fast      FastFieldNormalizer `json:"fast" yaml:"fast"`
	Tokenizer FieldTokenizerType  `json:"tokenizer" yaml:"tokenizer"`
}

func (t TextFieldType) IsIndexed() bool      { return t.Indexed }
func (t TextFieldType) IsStaticObject() bool { return false }

// NumberType represents numeric types
type NumberType string

const (
	NumberTypeU64 NumberType = "u64"
	NumberTypeI64 NumberType = "i64"
	NumberTypeF64 NumberType = "f64"
)

// NumberFieldType represents a number field
type NumberFieldType struct {
	Type        NumberType          `json:"type" yaml:"type"`
	Stored      bool                `json:"stored" yaml:"stored"`
	Indexed     bool                `json:"indexed" yaml:"indexed"`
	Fast        FastFieldNormalizer `json:"fast" yaml:"fast"`
	ParseString bool                `json:"parse_string" yaml:"parse_string"`
}

func (n NumberFieldType) IsIndexed() bool      { return n.Indexed }
func (n NumberFieldType) IsStaticObject() bool { return false }

// BooleanFieldType represents a boolean field
type BooleanFieldType struct {
	Stored      bool                `json:"stored" yaml:"stored"`
	Indexed     bool                `json:"indexed" yaml:"indexed"`
	Fast        FastFieldNormalizer `json:"fast" yaml:"fast"`
	ParseString bool                `json:"parse_string" yaml:"parse_string"`
}

func (b BooleanFieldType) IsIndexed() bool      { return b.Indexed }
func (b BooleanFieldType) IsStaticObject() bool { return false }

// DatetimeFieldType represents a datetime field
type DatetimeFieldType struct {
	Stored  bool                `json:"stored" yaml:"stored"`
	Indexed bool                `json:"indexed" yaml:"indexed"`
	Fast    FastFieldNormalizer `json:"fast" yaml:"fast"`
	Formats DateTimeFormats     `json:"formats" yaml:"formats"`
}

func (d DatetimeFieldType) IsIndexed() bool      { return d.Indexed }
func (d DatetimeFieldType) IsStaticObject() bool { return false }

// IPFieldType represents an IP address field
type IPFieldType struct {
	Stored  bool                `json:"stored" yaml:"stored"`
	Indexed bool                `json:"indexed" yaml:"indexed"`
	Fast    FastFieldNormalizer `json:"fast" yaml:"fast"`
}

func (i IPFieldType) IsIndexed() bool      { return i.Indexed }
func (i IPFieldType) IsStaticObject() bool { return false }

// DynamicObjectFieldType represents a dynamic object field
type DynamicObjectFieldType struct {
	Config DynamicObjectFieldConfig `json:"config" yaml:"config"`
}

func (d DynamicObjectFieldType) IsIndexed() bool      { return true }
func (d DynamicObjectFieldType) IsStaticObject() bool { return false }

// StaticObjectFieldType represents a static object field
type StaticObjectFieldType struct {
	Fields FieldConfigs `json:"fields" yaml:"fields"`
}

func (s StaticObjectFieldType) IsIndexed() bool      { return false }
func (s StaticObjectFieldType) IsStaticObject() bool { return true }

// FastFieldNormalizer represents fast field normalization options
type FastFieldNormalizer interface {
	isFastFieldNormalizer()
}

type FastFieldNormalizerType string

const (
	FastFieldNormalizerTypeFalse FastFieldNormalizerType = "false"
	FastFieldNormalizerTypeTrue  FastFieldNormalizerType = "true"
)

func (f FastFieldNormalizerType) isFastFieldNormalizer() {}

// FieldTokenizerType represents tokenizer types
type FieldTokenizerType string

const (
	FieldTokenizerTypeDefault FieldTokenizerType = "default"
	FieldTokenizerTypeRaw     FieldTokenizerType = "raw"
)

// DynamicObjectFieldConfig represents configuration for dynamic object fields
type DynamicObjectFieldConfig struct {
	Stored     bool                          `json:"stored" yaml:"stored"`
	Fast       FastFieldNormalizerType       `json:"fast" yaml:"fast"`
	Indexed    IndexedDynamicObjectFieldType `json:"indexed" yaml:"indexed"`
	ExpandDots bool                          `json:"expand_dots" yaml:"expand_dots"`
}

// IndexedDynamicObjectFieldType represents indexed dynamic object configuration
type IndexedDynamicObjectFieldType struct {
	Config IndexedDynamicObjectFieldConfig `json:"config" yaml:"config"`
}

// IndexedDynamicObjectFieldConfig represents the configuration for indexed dynamic objects
type IndexedDynamicObjectFieldConfig struct {
	Record    string             `json:"record" yaml:"record"`
	Tokenizer FieldTokenizerType `json:"tokenizer" yaml:"tokenizer"`
}

// NewIndexedDynamicObjectFieldType creates a new indexed dynamic object field type
func NewIndexedDynamicObjectFieldType() IndexedDynamicObjectFieldType {
	return IndexedDynamicObjectFieldType{
		Config: IndexedDynamicObjectFieldConfig{
			Record:    "basic",
			Tokenizer: FieldTokenizerTypeDefault,
		},
	}
}

// DateTimeFormats represents date/time parsing formats
type DateTimeFormats struct {
	Formats []string `json:"formats" yaml:"formats"`
}

// TryParse attempts to parse a value using the configured formats
func (dtf DateTimeFormats) TryParse(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string for datetime field")
	}

	for _, format := range dtf.Formats {
		if parsed, err := time.Parse(format, str); err == nil {
			return parsed, nil
		}
	}

	return nil, fmt.Errorf("failed to parse datetime value: %s", str)
}

// FieldConfigs represents a slice of field configurations
type FieldConfigs []FieldConfig

// GetIndexed returns only the indexed fields from the field configs
func (fields FieldConfigs) GetIndexed() []FieldConfig {
	var indexed []FieldConfig
	for _, field := range fields {
		if field.Type.IsIndexed() {
			indexed = append(indexed, field)
		}
	}
	return indexed
}

// LoadIndexConfigFromPath loads an index configuration from a file path
func LoadIndexConfigFromPath(path string) (*IndexConfig, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config IndexConfig

	// Try YAML first, then JSON
	if err := yaml.Unmarshal(data, &config); err != nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, fmt.Errorf("failed to parse config as YAML or JSON: %w", err)
		}
	}

	return &config, nil
}

// EscapedWithParentName creates an escaped field name with optional parent
func EscapedWithParentName(name string, parentName *string) string {
	if parentName == nil {
		return name
	}
	return fmt.Sprintf("%s.%s", *parentName, name)
}

// SplitObjectFieldName splits a field name by dots
func SplitObjectFieldName(name string) []string {
	return strings.Split(name, ".")
}

// UnescapedFieldName returns the unescaped version of a field name
func UnescapedFieldName(name string) string {
	// For now, just return the name as-is
	// In a full implementation, this would handle escape sequences
	return name
}
