package unified_index

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FileReader represents a file reader
// Equivalent to FileReader struct in Rust
type FileReader struct {
	reader   io.Reader
	fileName string
}

// NewFileReaderFromPath creates a new FileReader from a file path
// Equivalent to from_path method in Rust
func NewFileReaderFromPath(dir, fileName string) (*FileReader, error) {
	file, err := os.Open(filepath.Join(dir, fileName))
	if err != nil {
		return nil, err
	}

	return &FileReader{
		reader:   file,
		fileName: fileName,
	}, nil
}

// NewFileReader creates a new FileReader
// Equivalent to new method in Rust
func NewFileReader(reader io.Reader, fileName string) *FileReader {
	return &FileReader{
		reader:   reader,
		fileName: fileName,
	}
}

// UnifiedIndexWriter represents a unified index writer
// Equivalent to UnifiedIndexWriter struct in Rust
type UnifiedIndexWriter struct {
	fileReaders []*FileReader
	fileOffsets map[string]Range
}

// NewUnifiedIndexWriterFromFilePaths creates a new UnifiedIndexWriter from file paths
// Equivalent to from_file_paths method in Rust
func NewUnifiedIndexWriterFromFilePaths(dir string, fileNames []string) (*UnifiedIndexWriter, error) {
	var fileReaders []*FileReader

	for _, fileName := range fileNames {
		reader, err := NewFileReaderFromPath(dir, fileName)
		if err != nil {
			return nil, err
		}
		fileReaders = append(fileReaders, reader)
	}

	return NewUnifiedIndexWriter(fileReaders), nil
}

// NewUnifiedIndexWriter creates a new UnifiedIndexWriter
// Equivalent to new method in Rust
func NewUnifiedIndexWriter(fileReaders []*FileReader) *UnifiedIndexWriter {
	return &UnifiedIndexWriter{
		fileReaders: fileReaders,
		fileOffsets: make(map[string]Range),
	}
}

// Write writes the unified index to a writer
// Equivalent to write method in Rust
func (uiw *UnifiedIndexWriter) Write(writer io.Writer, cache FileCache) (uint64, uint64, error) {
	var written uint64

	for _, fileReader := range uiw.fileReaders {
		start := written
		fileName := fileReader.fileName

		// Copy file content to writer
		n, err := io.Copy(writer, fileReader.reader)
		if err != nil {
			return 0, 0, err
		}

		written += uint64(n)
		uiw.fileOffsets[fileName] = NewRange(start, written)
	}

	// Write footer
	footer := NewIndexFooter(uiw.fileOffsets, cache)
	footerBytes, err := serializeFooter(footer)
	if err != nil {
		return 0, 0, err
	}

	footerLen := uint64(len(footerBytes))
	footerWritten, err := writer.Write(footerBytes)
	if err != nil {
		return 0, 0, err
	}

	if uint64(footerWritten) < footerLen {
		return 0, 0, fmt.Errorf("written less than expected: %d < %d", footerWritten, footerLen)
	}

	return written + footerLen, footerLen, nil
}

// WriteWithoutCache writes the unified index without cache (for testing)
// Equivalent to write_without_cache method in Rust
func (uiw *UnifiedIndexWriter) WriteWithoutCache(writer io.Writer) (uint64, uint64, error) {
	emptyCache := make(FileCache)
	return uiw.Write(writer, emptyCache)
}

// serializeFooter serializes the footer to bytes
// Uses JSON serialization for compatibility with Rust bincode equivalent
func serializeFooter(footer *IndexFooter) ([]byte, error) {
	data, err := json.Marshal(footer)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize footer: %w", err)
	}
	return data, nil
}
