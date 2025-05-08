package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/model"
	"github.com/sirupsen/logrus"
)

// ClickHouseService defines ClickHouse operations
type ClickHouseService interface {
	Connect(ctx context.Context, params model.ClickHouseConnectionParams, token string) error
	ListTables(ctx context.Context) ([]string, error)
	GetTableColumns(ctx context.Context, tableName string) ([]model.Column, error)
	PreviewData(ctx context.Context, tableName string, columns []string, limit int) ([]map[string]interface{}, error)
	BuildJoinQuery(params model.JoinParams) (string, error)
	ExecuteJoinPreview(ctx context.Context, query string, limit int) ([]map[string]interface{}, error)
	ExecuteQuery(ctx context.Context, query string, progressCh chan<- model.ProgressUpdate) (int, error)
	CreateTable(ctx context.Context, tableName string, columns []model.Column) error
	InsertData(ctx context.Context, tableName string, columns []model.Column, data <-chan []interface{}, progressCh chan<- model.ProgressUpdate) (int, error)
}

// ClickHouseServiceImpl implements ClickHouseService
type ClickHouseServiceImpl struct {
	conn   driver.Conn
	config *config.Config
	logger *logrus.Logger
}

// NewClickHouseService creates a new ClickHouse service
func NewClickHouseService(config *config.Config, logger *logrus.Logger) ClickHouseService {
	return &ClickHouseServiceImpl{
		config: config,
		logger: logger,
	}
}

// Connect establishes a connection to ClickHouse
func (s *ClickHouseServiceImpl) Connect(ctx context.Context, params model.ClickHouseConnectionParams, token string) error {
	// Create options with JWT token auth
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", params.Host, params.Port)},
		Auth: clickhouse.Auth{
			Database: params.Database,
			Username: params.User,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:          10 * time.Second,
		MaxOpenConns:         5,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Hour,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10 * 1024 * 1024,
	}

	// If token is provided, configure JWT auth
	if token != "" {
		options.Auth.AccessToken = token
	}

	// Connect to ClickHouse
	conn, err := clickhouse.Open(options)
	if err != nil {
		return fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}

	// Test connection
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	s.conn = conn
	return nil
}

// ListTables returns a list of tables in the connected database
func (s *ClickHouseServiceImpl) ListTables(ctx context.Context) ([]string, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("not connected to ClickHouse")
	}

	query := "SHOW TABLES"
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return tables, nil
}

// GetTableColumns returns the columns of a table
func (s *ClickHouseServiceImpl) GetTableColumns(ctx context.Context, tableName string) ([]model.Column, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("not connected to ClickHouse")
	}

	query := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var columns []model.Column
	for rows.Next() {
		var name, dataType, defaultType, defaultExpression string
		var comment interface{}
		if err := rows.Scan(&name, &dataType, &defaultType, &defaultExpression, &comment); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns = append(columns, model.Column{
			Name: name,
			Type: dataType,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return columns, nil
}

// PreviewData returns a preview of the data
func (s *ClickHouseServiceImpl) PreviewData(ctx context.Context, tableName string, columns []string, limit int) ([]map[string]interface{}, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("not connected to ClickHouse")
	}

	// Build query
	columnStr := "*"
	if len(columns) > 0 {
		columnStr = strings.Join(columns, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d", columnStr, tableName, limit)

	// Execute query
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names and types
	columnNames := rows.ColumnNames()
	columnTypes := rows.ColumnTypes()

	
	// Prepare result
	result := make([]map[string]interface{}, 0, limit)

	// Iterate through rows
	for rows.Next() {
		// Create a slice for row values
		rowValues := make([]interface{}, len(columnNames))
		rowPointers := make([]interface{}, len(columnNames))
		for i := range rowValues {
			rowPointers[i] = &rowValues[i]
		}

		// Scan row into slice
		if err := rows.Scan(rowPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create map for row
		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			rowMap[colName] = rowValues[i]
		}

		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

// BuildJoinQuery builds a JOIN query from JoinParams
func (s *ClickHouseServiceImpl) BuildJoinQuery(params model.JoinParams) (string, error) {
	if len(params.Tables) < 2 {
		return "", fmt.Errorf("at least two tables are required for a join")
	}

	// Main table
	mainTable := params.Tables[0]
	
	// Build selected columns
	allColumns := make([]string, 0)
	for i, table := range params.Tables {
		for _, col := range table.SelectedColumns {
			// Add table prefix to avoid ambiguity
			allColumns = append(allColumns, fmt.Sprintf("%s.%s", table.Name, col))
		}
	}
	
	if len(allColumns) == 0 {
		return "", fmt.Errorf("no columns selected")
	}
	
	// Start building query
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(allColumns, ", "), mainTable.Name)
	
	// Add joins
	for i := 1; i < len(params.Tables); i++ {
		joinTable := params.Tables[i]
		joinType := "INNER JOIN"
		if joinTable.JoinType != "" {
			joinType = joinTable.JoinType
		}
		
		// Get join condition
		if joinTable.JoinCondition == "" {
			return "", fmt.Errorf("join condition is required for table %s", joinTable.Name)
		}
		
		query += fmt.Sprintf(" %s %s ON %s", joinType, joinTable.Name, joinTable.JoinCondition)
	}
	
	// Add where clause if provided
	if params.WhereClause != "" {
		query += " WHERE " + params.WhereClause
	}
	
	return query, nil
}

// ExecuteJoinPreview executes a join query and returns preview data
func (s *ClickHouseServiceImpl) ExecuteJoinPreview(ctx context.Context, query string, limit int) ([]map[string]interface{}, error) {
	if s.conn == nil {
		return nil, fmt.Errorf("not connected to ClickHouse")
	}
	
	// Add limit to query
	query = query + fmt.Sprintf(" LIMIT %d", limit)
	
	// Execute query
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	
	// Get column names
	columnNames := rows.ColumnNames()
	
	// Prepare result
	result := make([]map[string]interface{}, 0, limit)
	
	// Iterate through rows
	for rows.Next() {
		// Create a slice for row values
		rowValues := make([]interface{}, len(columnNames))
		rowPointers := make([]interface{}, len(columnNames))
		for i := range rowValues {
			rowPointers[i] = &rowValues[i]
		}
		
		// Scan row into slice
		if err := rows.Scan(rowPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		
		// Create map for row
		rowMap := make(map[string]interface{})
		for i, colName := range columnNames {
			rowMap[colName] = rowValues[i]
		}
		
		result = append(result, rowMap)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return result, nil
}

// ExecuteQuery executes a query and streams results through a channel
func (s *ClickHouseServiceImpl) ExecuteQuery(ctx context.Context, query string, progressCh chan<- model.ProgressUpdate) (int, error) {
	if s.conn == nil {
		return 0, fmt.Errorf("not connected to ClickHouse")
	}
	
	// Execute query
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	
	// Get column names
	columnNames := rows.ColumnNames()
	
	// Process rows
	totalRows := 0
	batchSize := s.config.BatchSize
	progressReportSize := s.config.ProgressReportSize
	
	for rows.Next() {
		// Create a slice for row values
		rowValues := make([]interface{}, len(columnNames))
		rowPointers := make([]interface{}, len(columnNames))
		for i := range rowValues {
			rowPointers[i] = &rowValues[i]
		}
		
		// Scan row into slice
		if err := rows.Scan(rowPointers...); err != nil {
			return totalRows, fmt.Errorf("failed to scan row: %w", err)
		}
		
		totalRows++
		
		// Report progress periodically
		if totalRows%progressReportSize == 0 {
			select {
			case progressCh <- model.ProgressUpdate{
				Status:    "processing",
				Message:   fmt.Sprintf("Processed %d rows", totalRows),
				Count:     totalRows,
				Completed: false,
			}:
			case <-ctx.Done():
				return totalRows, ctx.Err()
			}
		}
	}
	
	if err := rows.Err(); err != nil {
		return totalRows, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return totalRows, nil
}

// CreateTable creates a new table in ClickHouse
func (s *ClickHouseServiceImpl) CreateTable(ctx context.Context, tableName string, columns []model.Column) error {
	if s.conn == nil {
		return fmt.Errorf("not connected to ClickHouse")
	}
	
	// Build column definitions
	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDefs[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	
	// Build create table query
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple()",
		tableName,
		strings.Join(columnDefs, ", "),
	)
	
	// Execute query
	if err := s.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	
	return nil
}

// InsertData inserts data into a table
func (s *ClickHouseServiceImpl) InsertData(
	ctx context.Context,
	tableName string,
	columns []model.Column,
	data <-chan []interface{},
	progressCh chan<- model.ProgressUpdate,
) (int, error) {
	if s.conn == nil {
		return 0, fmt.Errorf("not connected to ClickHouse")
	}
	
	// Get column names
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	
	// Prepare insert statement
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES",
		tableName,
		strings.Join(columnNames, ", "),
	)
	
	// Insert data in batches
	totalRows := 0
	batch := make([][]interface{}, 0, s.config.BatchSize)
	progressReportSize := s.config.ProgressReportSize
	lastReportedCount := 0
	
	for rowData := range data {
		batch = append(batch, rowData)
		
		// If batch is full, insert it
		if len(batch) >= s.config.BatchSize {
			// Insert batch
			if err := s.conn.AsyncInsert(ctx, query, batch, false); err != nil {
				return totalRows, fmt.Errorf("failed to insert batch: %w", err)
			}
			
			totalRows += len(batch)
			batch = make([][]interface{}, 0, s.config.BatchSize)
			
			// Report progress if needed
			if totalRows-lastReportedCount >= progressReportSize {
				select {
				case progressCh <- model.ProgressUpdate{
					Status:    "processing",
					Message:   fmt.Sprintf("Inserted %d rows", totalRows),
					Count:     totalRows,
					Completed: false,
				}:
					lastReportedCount = totalRows
				case <-ctx.Done():
					return totalRows, ctx.Err()
				}
			}
		}
	}
	
	// Insert any remaining rows
	if len(batch) > 0 {
		if err := s.conn.AsyncInsert(ctx, query, batch, false); err != nil {
			return totalRows, fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalRows += len(batch)
	}
	
	return totalRows, nil
}