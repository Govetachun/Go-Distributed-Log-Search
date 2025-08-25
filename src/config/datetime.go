package config

import (
	"fmt"
	"strconv"
	"time"
)

// DateTimeFormatType represents date/time parsing formats
// Equivalent to DateTimeFormatType enum in Rust
type DateTimeFormatType string

const (
	DateTimeFormatTypeIso8601   DateTimeFormatType = "iso8601"
	DateTimeFormatTypeRfc2822   DateTimeFormatType = "rfc2822"
	DateTimeFormatTypeRfc3339   DateTimeFormatType = "rfc3339"
	DateTimeFormatTypeTimestamp DateTimeFormatType = "timestamp"
	DateTimeFormatTypeDate      DateTimeFormatType = "date"
)

// ParseTimestamp parses a timestamp value
// Equivalent to parse_timestamp function in Rust
func ParseTimestamp(timestamp int64) (time.Time, error) {
	// Minimum supported timestamp value in seconds (13 Apr 1972 23:59:55 GMT).
	const MIN_TIMESTAMP_SECONDS = 72057595

	// Maximum supported timestamp value in seconds (16 Mar 2242 12:56:31 GMT).
	const MAX_TIMESTAMP_SECONDS = 8589934591

	const MIN_TIMESTAMP_MILLIS = MIN_TIMESTAMP_SECONDS * 1000
	const MAX_TIMESTAMP_MILLIS = MAX_TIMESTAMP_SECONDS * 1000
	const MIN_TIMESTAMP_MICROS = MIN_TIMESTAMP_SECONDS * 1000000
	const MAX_TIMESTAMP_MICROS = MAX_TIMESTAMP_SECONDS * 1000000
	const MIN_TIMESTAMP_NANOS = MIN_TIMESTAMP_SECONDS * 1000000000
	const MAX_TIMESTAMP_NANOS = MAX_TIMESTAMP_SECONDS * 1000000000

	switch {
	case timestamp >= MIN_TIMESTAMP_SECONDS && timestamp <= MAX_TIMESTAMP_SECONDS:
		return time.Unix(timestamp, 0), nil
	case timestamp >= MIN_TIMESTAMP_MILLIS && timestamp <= MAX_TIMESTAMP_MILLIS:
		return time.Unix(timestamp/1000, (timestamp%1000)*1000000), nil
	case timestamp >= MIN_TIMESTAMP_MICROS && timestamp <= MAX_TIMESTAMP_MICROS:
		return time.Unix(timestamp/1000000, (timestamp%1000000)*1000), nil
	case timestamp >= MIN_TIMESTAMP_NANOS && timestamp <= MAX_TIMESTAMP_NANOS:
		return time.Unix(timestamp/1000000000, timestamp%1000000000), nil
	default:
		return time.Time{}, fmt.Errorf("failed to parse unix timestamp `%d`. Supported timestamp ranges from `13 Apr 1972 23:59:55` to `16 Mar 2242 12:56:31`", timestamp)
	}
}

// TryParse attempts to parse a value using the configured format
// Equivalent to try_parse method in Rust
func (dtf DateTimeFormatType) TryParse(value interface{}) (interface{}, error) {
	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string for datetime field")
	}

	switch dtf {
	case DateTimeFormatTypeIso8601:
		parsed, err := time.Parse(time.RFC3339, str)
		if err == nil {
			return parsed, nil
		}
	case DateTimeFormatTypeRfc2822:
		parsed, err := time.Parse(time.RFC1123, str)
		if err == nil {
			return parsed, nil
		}
	case DateTimeFormatTypeRfc3339:
		parsed, err := time.Parse(time.RFC3339, str)
		if err == nil {
			return parsed, nil
		}
	case DateTimeFormatTypeTimestamp:
		if timestamp, err := strconv.ParseInt(str, 10, 64); err == nil {
			return ParseTimestamp(timestamp)
		}
	case DateTimeFormatTypeDate:
		parsed, err := time.Parse("2006-01-02", str)
		if err == nil {
			return parsed, nil
		}
	}

	return nil, fmt.Errorf("failed to parse datetime value: %s", str)
}

// DateTimeFormats represents date/time parsing formats
// Equivalent to DateTimeFormats struct in Rust
type DateTimeFormats []DateTimeFormatType

// Default returns the default DateTimeFormats
// Equivalent to Default implementation in Rust
func DefaultDateTimeFormats() DateTimeFormats {
	return DateTimeFormats{
		DateTimeFormatTypeDate,
		DateTimeFormatTypeRfc3339,
		DateTimeFormatTypeTimestamp,
	}
}

// TryParse attempts to parse a value using the configured formats
// Equivalent to try_parse method in Rust
func (dtf DateTimeFormats) TryParse(value interface{}) (interface{}, error) {
	for _, format := range dtf {
		if parsed, err := format.TryParse(value); err == nil {
			return parsed, nil
		}
	}
	return nil, fmt.Errorf("none of the datetime formats was able to parse")
}

// DateTimeFastPrecisionType represents date/time fast precision types
// Equivalent to DateTimeFastPrecisionType enum in Rust
type DateTimeFastPrecisionType string

const (
	DateTimeFastPrecisionTypeFalse        DateTimeFastPrecisionType = "false"
	DateTimeFastPrecisionTypeTrue         DateTimeFastPrecisionType = "true"
	DateTimeFastPrecisionTypeSeconds      DateTimeFastPrecisionType = "seconds"
	DateTimeFastPrecisionTypeMilliseconds DateTimeFastPrecisionType = "milliseconds"
	DateTimeFastPrecisionTypeMicroseconds DateTimeFastPrecisionType = "microseconds"
	DateTimeFastPrecisionTypeNanoseconds  DateTimeFastPrecisionType = "nanoseconds"
)

// From converts DateTimeFastPrecisionType to precision option
// Equivalent to From<DateTimeFastPrecisionType> for Option<DateTimePrecision> in Rust
func (d DateTimeFastPrecisionType) From() *string {
	switch d {
	case DateTimeFastPrecisionTypeFalse:
		return nil
	case DateTimeFastPrecisionTypeTrue, DateTimeFastPrecisionTypeSeconds:
		s := "seconds"
		return &s
	case DateTimeFastPrecisionTypeMilliseconds:
		s := "milliseconds"
		return &s
	case DateTimeFastPrecisionTypeMicroseconds:
		s := "microseconds"
		return &s
	case DateTimeFastPrecisionTypeNanoseconds:
		s := "nanoseconds"
		return &s
	default:
		return nil
	}
}

// DateTimeFieldConfig represents a datetime field configuration
// Equivalent to DateTimeFieldConfig struct in Rust
type DateTimeFieldConfig struct {
	Stored  bool                      `json:"stored" yaml:"stored"`
	Indexed bool                      `json:"indexed" yaml:"indexed"`
	Fast    DateTimeFastPrecisionType `json:"fast" yaml:"fast"`
	Formats DateTimeFormats           `json:"formats" yaml:"formats"`
}

// NewDateTimeFieldConfig creates a new DateTimeFieldConfig with defaults
func NewDateTimeFieldConfig() *DateTimeFieldConfig {
	return &DateTimeFieldConfig{
		Stored:  defaultTrue(),
		Indexed: defaultTrue(),
		Fast:    DateTimeFastPrecisionTypeFalse,
		Formats: DefaultDateTimeFormats(),
	}
}

// IsIndexed implements FieldType interface
func (d DateTimeFieldConfig) IsIndexed() bool {
	return d.Indexed
}

// ToDateOptions converts DateTimeFieldConfig to DateOptions equivalent
// Equivalent to From<DateTimeFieldConfig> for DateOptions in Rust
func (d DateTimeFieldConfig) ToDateOptions() map[string]interface{} {
	options := make(map[string]interface{})
	if d.Stored {
		options["stored"] = true
	}
	if d.Indexed {
		options["indexed"] = true
	}
	if precision := d.Fast.From(); precision != nil {
		options["fast"] = true
		options["precision"] = *precision
	}
	return options
}
