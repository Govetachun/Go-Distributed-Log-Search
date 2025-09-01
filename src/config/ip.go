package config

// IpFieldConfig represents an IP address field configuration
type IpFieldConfig struct {
	Stored  bool `json:"stored" yaml:"stored"`
	Fast    bool `json:"fast" yaml:"fast"`
	Indexed bool `json:"indexed" yaml:"indexed"`
}

// NewIpFieldConfig creates a new IpFieldConfig with defaults
func NewIpFieldConfig() *IpFieldConfig {
	return &IpFieldConfig{
		Stored:  defaultTrue(),
		Fast:    false,
		Indexed: defaultTrue(),
	}
}

// IsIndexed implements FieldType interface
func (i IpFieldConfig) IsIndexed() bool {
	return i.Indexed
}

// ToIpAddrOptions converts IpFieldConfig to IpAddrOptions equivalent
func (i IpFieldConfig) ToIpAddrOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if i.Stored {
		options["stored"] = true
	}
	if i.Indexed {
		options["indexed"] = true
	}
	if i.Fast {
		options["fast"] = true
	}
	return options
}
