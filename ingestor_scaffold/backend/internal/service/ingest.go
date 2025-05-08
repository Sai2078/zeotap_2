package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/model"
	"github.com/sirupsen/logrus"
)

// IngestService defines ingestion operations
type IngestService interface {
	IngestClickHouseToFlatFile(
		ctx context.Context,
		tableName string,
		columns []model.Column,
		flatFileParams model.FlatFileParams,
		query string,
		progressCh chan<- model.ProgressUpdate,
	) (model.IngestionResult, error)
	
	IngestFlatFileToClickHouse(
		ctx context.Context,
		flatFileParams model.FlatFileParams,
		tableName string,
		columns []model.Column,
		progressCh chan<- model.ProgressUpdate,
	) (model.IngestionResult, error)
}

// IngestServiceImpl implements IngestService
type IngestServiceImpl struct {
	clickhouseService ClickHouseService
	flatFileService   FlatFileService
	config            *config.Config
	logger            *logrus.Logger
}

// NewIngestService creates a new ingest service
func NewIngestService(
	clickhouseService ClickHouseService,
	flatFileService FlatFileService,
	config *config.Config,
	logger *logrus.Logger,
) IngestService {
	return &IngestServiceImpl{
		clickhouseService: clickhouseService,
		flatFileService:   flatFileService,
		config:            config,
		logger:            logger,
	}
}

// IngestClickHouseToFlatFile ingests data from ClickHouse to a flat file
func (s *IngestServiceImpl) IngestClickHouseToFlatFile(
	ctx context.Context,
	tableName string,
	columns []model.Column,
	flatFileParams model.FlatFileParams,
	query string,
	progressCh chan<- model.ProgressUpdate,
) (model.IngestionResult, error) {
	// Build query if not provided
	if query == "" {
		// Extract column names
		columnNames := make([]string, len(columns))
		for i, col := range columns {
			columnNames[i] = col.Name
		}
		
		query = fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), tableName)
	}
	
	// Channel for intermediate data
	dataCh := make(chan map[string]interface{}, 100)
	
	// Start goroutine to fetch data from ClickHouse
	go func() {
		defer close(dataCh)
		
		// Execute query
		rows, err := s.clickhouseService.conn.Query(ctx, query)
		if err != nil {
			s.logger.WithError(err).Error("Failed to execute query")
			progressCh <- model.ProgressUpdate{
				Status:    "error",
				Message:   "Failed to execute query: " + err.Error(),
				Count:     0,
				Completed: true,
			}
			return
		}
		defer rows.Close()
		
		// Get column names
		columnNames := rows.ColumnNames()
		
		// Process rows
		totalRows := 0
		progressReportSize := s.config.ProgressReportSize
		
		for rows.Next() {
			// Check context for cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}
			
			// Create a slice for row values
			rowValues := make([]interface{}, len(columnNames))
			rowPointers := make([]interface{}, len(columnNames))
			for i := range rowValues {
				rowPointers[i] = &rowValues[i]
			}
			
			// Scan row into slice
			if err := rows.Scan(rowPointers...); err != nil {
				s.logger.WithError(err).Error("Failed to scan row")
				continue
			}
			
			// Create map for row
			rowMap := make(map[string]interface{})
			for i, colName := range columnNames {
				rowMap[colName] = rowValues[i]
			}
			
			// Send row to channel
			select {
			case dataCh <- rowMap:
			case <-ctx.Done():
				return
			}
			
			totalRows++
			
			// Report progress periodically
			if totalRows%progressReportSize == 0 {
				select {
				case progressCh <- model.ProgressUpdate{
					Status:    "processing",
					Message:   fmt.Sprintf("Fetched %d rows", totalRows),
					Count:     totalRows,
					Completed: false,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
		
		if err := rows.Err(); err != nil {
			s.logger.WithError(err).Error("Error iterating rows")
			progressCh <- model.ProgressUpdate{
				Status:    "error",
				Message:   "Error iterating rows: " + err.Error(),
				Count:     totalRows,
				Completed: true,
			}
			return
		}
	}()
	
	// Write data to flat file
	count, err := s.flatFileService.WriteData(
		ctx,
		flatFileParams.FilePath,
		flatFileParams.Delimiter,
		columns,
		dataCh,
		progressCh,
	)
	
	if err != nil {
		return model.IngestionResult{}, err
	}
	
	return model.IngestionResult{
		TotalRecords: count,
	}, nil
}

// IngestFlatFileToClickHouse ingests data from a flat file to ClickHouse
func (s *IngestServiceImpl) IngestFlatFileToClickHouse(
	ctx context.Context,
	flatFileParams model.FlatFileParams,
	tableName string,
	columns []model.Column,
	progressCh chan<- model.ProgressUpdate,
) (model.IngestionResult, error) {
	// Create table if it doesn't exist
	if err := s.clickhouseService.CreateTable(ctx, tableName, columns); err != nil {
		return model.IngestionResult{}, fmt.Errorf("failed to create table: %w", err)
	}
	
	// Read data from flat file
	dataCh, err := s.flatFileService.ReadData(
		ctx,
		flatFileParams.FilePath,
		flatFileParams.Delimiter,
		columns,
	)
	if err != nil {
		return model.IngestionResult{}, fmt.Errorf("failed to read data: %w", err)
	}
	
	// Insert data into ClickHouse
	count, err := s.clickhouseService.InsertData(
		ctx,
		tableName,
		columns,
		dataCh,
		progressCh,
	)
	
	if err != nil {
		return model.IngestionResult{}, fmt.Errorf("failed to insert data: %w", err)
	}
	
	return model.IngestionResult{
		TotalRecords: count,
	}, nil
}