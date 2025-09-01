package unified_index

const VERSION = 1

// IndexFooter represents the index footer structure
type IndexFooter struct {
	FileOffsets map[string]Range `json:"file_offsets" yaml:"file_offsets"`
	Cache       FileCache        `json:"cache" yaml:"cache"`
	Version     uint32           `json:"version" yaml:"version"`
}

// NewIndexFooter creates a new IndexFooter
func NewIndexFooter(fileOffsets map[string]Range, cache FileCache) *IndexFooter {
	return &IndexFooter{
		FileOffsets: fileOffsets,
		Cache:       cache,
		Version:     VERSION,
	}
}

// Range represents a byte range
type Range struct {
	Start uint64 `json:"start" yaml:"start"`
	End   uint64 `json:"end" yaml:"end"`
}

// NewRange creates a new Range.
func NewRange(start, end uint64) Range {
	return Range{
		Start: start,
		End:   end,
	}
}

// Contains checks if a position is within the range
func (r Range) Contains(pos uint64) bool {
	return pos >= r.Start && pos < r.End
}

// Length returns the length of the range
func (r Range) Length() uint64 {
	return r.End - r.Start
}
