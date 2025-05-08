package service

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/model"
	"github.com/sirupsen/logrus"
)

// FlatFileService defines operations for flat files
type FlatFileService interface {
	DiscoverSchema(ctx context.Context, filePath, delimiter string) ([]model.Column, error)
	PreviewData(ctx context.Context, filePath, delimiter string, columns []model.Column, limit int) ([]map[string]interface{}, error)
	ReadData(ctx context.Context, filePath, delimiter string, columns []model.Column) (<-chan []interface{}, error)
	WriteData(ctx context.Context, filePath, delimiter string, columns []model.Column, data <-chan map[string]interface{}, progressCh chan<- model.ProgressUpdate) (int, error)
}

// FlatFileServiceImpl implements FlatFileService
type FlatFileServiceImpl struct {
	config *config.Config
	logger *logrus.Logger
}

// NewFlatFileService creates a new flat file service
func NewFlatFileService(config *config.Config, logger *logrus.Logger) FlatFileService {
	return &FlatFileServiceImpl{
		config: config,
		logger: logger,
	}
}

// DiscoverSchema discovers the schema of a flat file
func (s *FlatFileServiceImpl) DiscoverSchema(ctx context.Context, filePath, delimiter string) ([]model.Column, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	var delim rune = ','
	if delimiter != "" {
		delims := []rune(delimiter)
		if len(delims) > 0 {
			delim = delims[0]
		}
	}
	reader := csv.NewReader(file)
	reader.Comma = delim
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Create columns with empty types
	columns := make([]model.Column, len(header))
	for i, name := range header {
		columns[i] = model.Column{
			Name: name,
			Type: "",
		}
	}

	// Read sample rows to infer types
	sampleSize := 100
	types := make([][]string, len(header))
	for i := range types {
		types[i] = make([]string, 0, sampleSize)
	}

	// Read up to sampleSize rows
	for i := 0; i < sampleSize; i++ {
		// Check context for cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.WithError(err).Warn("Error reading row during schema discovery, skipping")
			continue
		}

		// Skip rows with different number of columns
		if len(record) != len(header) {
			continue
		}

		// Try to infer types
		for j, value := range record {
			valueType := s.inferType(value)
			types[j] = append(types[j], valueType)
		}
	}

	// Determine dominant type for each column
	for i, colTypes := range types {
		dominantType := s.getDominantType(colTypes)
		columns[i].Type = dominantType
	}

	return columns, nil
}

// inferType infers the data type of a value
func (s *FlatFileServiceImpl) inferType(value string) string {
	// Try to parse as int
	if value == "" {
		return "Nullable(String)" // Empty values can be any type, default to nullable string
	}

	// Try to parse as int
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "Int64"
	}

	// Try to parse as float
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "Float64"
	}

	// Try common date formats
	dateFormats := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"01-02-2006",
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		time.RFC3339,
	}

	for _, format := range dateFormats {
		if _, err := time.Parse(format, value); err == nil {
			return "DateTime"
		}
	}

	// Default to string
	return "String"
}

// getDominantType determines the dominant type from a list of types
func (s *FlatFileServiceImpl) getDominantType(types []string) string {
	if len(types) == 0 {
		return "String" // Default to string if no samples
	}

	// Count occurrences of each type
	counts := make(map[string]int)
	for _, t := range types {
		counts[t]++
	}

	// Find the most common type
	maxCount := 0
	dominantType := "String" // Default to string
	for t, count := range counts {
		if count > maxCount {
			maxCount = count
			dominantType = t
		}
	}

	// Special case: if we have Int64 and Float64, prefer Float64
	if counts["Int64"] > 0 && counts["Float64"] > 0 {
		return "Float64"
	}

	// Add Nullable if we have empty values
	if counts["Nullable(String)"] > 0 && dominantType != "String" {
		return "Nullable(" + dominantType + ")"
	}

	return dominantType
}

// PreviewData returns a preview of the data
func (s *FlatFileServiceImpl) PreviewData(
	ctx context.Context,
	filePath, delimiter string,
	columns []model.Column,
	limit int,
) ([]map[string]interface{}, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Create CSV reader
	var delim rune = ','
	if delimiter != "" {
		delims := []rune(delimiter)
		if len(delims) > 0 {
			delim = delims[0]
		}
	}
	reader := csv.NewReader(file)
	reader.Comma = delim
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Create column name to index map
	colNameToIndex := make(map[string]int)
	for i, name := range header {
		colNameToIndex[name] = i
	}

	// Filter columns if specified
	selectedColumns := columns
	if len(selectedColumns) == 0 {
		// If no columns specified, use all
		selectedColumns = make([]model.Column, len(header))
		for i, name := range header {
			selectedColumns[i] = model.Column{
				Name: name,
				Type: "String", // Default type
			}
		}
	}

	// Create result array
	result := make([]map[string]interface{}, 0, limit)

	// Read rows
	for i := 0; i < limit; i++ {
		// Check context for cancellation
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.WithError(err).Warn("Error reading row during preview, skipping")
			continue
		}

		// Skip rows with different number of columns
		if len(record) != len(header) {
			continue
		}

		// Create row map
		row := make(map[string]interface{})
		for _, col := range selectedColumns {
			idx, ok := colNameToIndex[col.Name]
			if !ok || idx >= len(record) {
				continue
			}

			// Convert value based on type
			value := record[idx]
			row[col.Name] = s.convertValue(value, col.Type)
		}

		result = append(result, row)
	}

	return result, nil
}

// ReadData reads data from a flat file and returns a channel of rows
func (s *FlatFileServiceImpl) ReadData(
	ctx context.Context,
	filePath, delimiter string,
	columns []model.Column,
) (<-chan []interface{}, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Create CSV reader
	var delim rune = ','
	if delimiter != "" {
		delims := []rune(delimiter)
		if len(delims) > 0 {
			delim = delims[0]
		}
	}
	reader := csv.NewReader(file)
	reader.Comma = delim
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Create column name to index map
	colNameToIndex := make(map[string]int)
	for i, name := range header {
		colNameToIndex[name] = i
	}

	// Create output channel
	out := make(chan []interface{}, 100)

	// Start goroutine to read data
	go func() {
		defer file.Close()
		defer close(out)

		for {
			// Check context for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				s.logger.WithError(err).Warn("Error reading row, skipping")
				continue
			}

			// Skip rows with different number of columns
			if len(record) != len(header) {
				continue
			}

			// Create row slice
			row := make([]interface{}, len(columns))
			for i, col := range columns {
				idx, ok := colNameToIndex[col.Name]
				if !ok || idx >= len(record) {
					row[i] = nil
					continue
				}

				// Convert value based on type
				value := record[idx]
				row[i] = s.convertValue(value, col.Type)
			}

			// Send row to channel
			select {
			case out <- row:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// WriteData writes data to a flat file
func (s *FlatFileServiceImpl) WriteData(
	ctx context.Context,
	filePath, delimiter string,
	columns []model.Column,
	data <-chan map[string]interface{},
	progressCh chan<- model.ProgressUpdate,
) (int, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create CSV writer
	var delim rune = ','
	if delimiter != "" {
		delims := []rune(delimiter)
		if len(delims) > 0 {
			delim = delims[0]
		}
	}
	writer := csv.NewWriter(file)
	writer.Comma = delim

	// Write header
	header := make([]string, len(columns))
	for i, col := range columns {
		header[i] = col.Name
	}
	if err := writer.Write(header); err != nil {
		return 0, fmt.Errorf("failed to write header: %w", err)
	}
	writer.Flush()

	// Write data
	totalRows := 0
	progressReportSize := s.config.ProgressReportSize
	lastReportedCount := 0

	for row := range data {
		// Check context for cancellation
		select {
		case <-ctx.Done():
			return totalRows, ctx.Err()
		default:
		}

		// Create record
		record := make([]string, len(columns))
		for i, col := range columns {
			value, ok := row[col.Name]
			if !ok {
				record[i] = ""
				continue
			}

			// Convert value to string
			record[i] = fmt.Sprintf("%v", value)
		}

		// Write record
		if err := writer.Write(record); err != nil {
			return totalRows, fmt.Errorf("failed to write record: %w", err)
		}

		totalRows++

		// Flush periodically
		if totalRows%1000 == 0 {
			writer.Flush()
			if err := writer.Error(); err != nil {
				return totalRows, fmt.Errorf("writer error: %w", err)
			}
		}

		// Report progress if needed
		if totalRows-lastReportedCount >= progressReportSize {
			select {
			case progressCh <- model.ProgressUpdate{
				Status:    "processing",
				Message:   fmt.Sprintf("Written %d rows", totalRows),
				Count:     totalRows,
				Completed: false,
			}:
				lastReportedCount = totalRows
			case <-ctx.Done():
				return totalRows, ctx.Err()
			}
		}
	}

	// Final flush
	writer.Flush()
	if err := writer.Error(); err != nil {
		return totalRows, fmt.Errorf("writer error: %w", err)
	}

	return totalRows, nil
}

// convertValue converts a string value to the appropriate type
func (s *FlatFileServiceImpl) convertValue(value string, dataType string) interface{} {
	// Handle nullable types
	if strings.HasPrefix(dataType, "Nullable(") && strings.HasSuffix(dataType, ")") {
		if value == "" {
			return nil
		}
		innerType := dataType[9 : len(dataType)-1]
		return s.convertValue(value, innerType)
	}

	switch dataType {
	case "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64":
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return i
		}
		return 0

	case "Float32", "Float64":
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			return f
		}
		return 0.0

	case "Bool":
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
		return false

	case "Date", "DateTime":
		// Try common date formats
		dateFormats := []string{
			"2006-01-02",
			"2006/01/02",
			"01/02/2006",
			"01-02-2006",
			"2006-01-02 15:04:05",
			"2006/01/02 15:04:05",
			time.RFC3339,
		}

		for _, format := range dateFormats {
			if t, err := time.Parse(format, value); err == nil {
				return t
			}
		}
		return time.Time{}

	default:
		return value
	}
}