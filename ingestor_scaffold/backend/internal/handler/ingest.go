package handler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/model"
	"github.com/ingestor/internal/service"
	"github.com/sirupsen/logrus"
)

// IngestHandler handles all ingestion related endpoints
type IngestHandler struct {
	clickhouseService service.ClickHouseService
	flatFileService   service.FlatFileService
	ingestService     service.IngestService
	cfg               *config.Config
	logger            *logrus.Logger
}

// NewIngestHandler creates a new ingest handler
func NewIngestHandler(
	clickhouseService service.ClickHouseService,
	flatFileService service.FlatFileService,
	ingestService service.IngestService,
	cfg *config.Config,
	logger *logrus.Logger,
) *IngestHandler {
	return &IngestHandler{
		clickhouseService: clickhouseService,
		flatFileService:   flatFileService,
		ingestService:     ingestService,
		cfg:               cfg,
		logger:            logger,
	}
}

// ConnectToClickHouse handles establishing connection to ClickHouse and fetching tables
func (h *IngestHandler) ConnectToClickHouse(c *gin.Context) {
	var params model.ClickHouseConnectionParams
	if err := c.ShouldBindJSON(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request body: " + err.Error(),
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get JWT token from request
	token := params.Token

	// Connect to ClickHouse
	err := h.clickhouseService.Connect(ctx, params, token)
	if err != nil {
		h.logger.WithError(err).Error("Failed to connect to ClickHouse")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to connect to ClickHouse: " + err.Error(),
		})
		return
	}

	// Get list of tables
	tables, err := h.clickhouseService.ListTables(ctx)
	if err != nil {
		h.logger.WithError(err).Error("Failed to list tables")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to list tables: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"tables": tables,
	})
}

// GetTableColumns returns the columns of a specific table
func (h *IngestHandler) GetTableColumns(c *gin.Context) {
	tableName := c.Param("tableName")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Table name is required",
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	// Get columns
	columns, err := h.clickhouseService.GetTableColumns(ctx, tableName)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get table columns")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to get table columns: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"columns": columns,
	})
}

// DiscoverFlatFileSchema discovers the schema of a flat file
func (h *IngestHandler) DiscoverFlatFileSchema(c *gin.Context) {
	var params model.FlatFileParams
	if err := c.ShouldBindJSON(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request body: " + err.Error(),
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// Discover schema
	columns, err := h.flatFileService.DiscoverSchema(ctx, params.FilePath, params.Delimiter)
	if err != nil {
		h.logger.WithError(err).Error("Failed to discover flat file schema")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to discover schema: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"columns": columns,
	})
}

// PreviewData allows previewing data before ingestion
func (h *IngestHandler) PreviewData(c *gin.Context) {
	var params model.PreviewParams
	if err := c.ShouldBindJSON(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request body: " + err.Error(),
		})
		return
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	var previewData []map[string]interface{}
	var err error

	switch params.SourceType {
	case "clickhouse":
		// Extract column names
		columnNames := make([]string, len(params.Columns))
		for i, col := range params.Columns {
			columnNames[i] = col.Name
		}

		// Preview data from ClickHouse
		previewData, err = h.clickhouseService.PreviewData(ctx, params.TableName, columnNames, h.cfg.MaxPreviewRows)
	case "flatfile":
		// Preview data from flat file
		previewData, err = h.flatFileService.PreviewData(ctx, params.FilePath, params.Delimiter, params.Columns, h.cfg.MaxPreviewRows)
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid source type",
		})
		return
	}

	if err != nil {
		h.logger.WithError(err).Error("Failed to preview data")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to preview data: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data":   previewData,
		"count":  len(previewData),
	})
}

// StartIngestion initiates the ingestion process
func (h *IngestHandler) StartIngestion(c *gin.Context) {
	var params model.IngestionParams
	if err := c.ShouldBindJSON(&params); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  "error",
			"message": "Invalid request body: " + err.Error(),
		})
		return
	}

	// Create a new context that's canceled if client disconnects
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	// Setup SSE response
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("Transfer-Encoding", "chunked")
	c.Writer.WriteHeader(http.StatusOK)

	// Create a progress channel
	progressCh := make(chan model.ProgressUpdate, 10)
	
	// Start ingestion in a goroutine
	go func() {
		var result model.IngestionResult
		var err error

		switch {
		case params.SourceType == "clickhouse" && params.TargetType == "flatfile":
			// ClickHouse to Flat File
			result, err = h.ingestService.IngestClickHouseToFlatFile(
				ctx,
				params.TableName,
				params.Columns,
				params.FlatFileParams,
				params.Query,
				progressCh,
			)
		case params.SourceType == "flatfile" && params.TargetType == "clickhouse":
			// Flat File to ClickHouse
			result, err = h.ingestService.IngestFlatFileToClickHouse(
				ctx,
				params.FlatFileParams,
				params.TableName,
				params.Columns,
				progressCh,
			)
		default:
			err = fmt.Errorf("invalid source or target type")
		}

		// Send final result or error
		if err != nil {
			h.logger.WithError(err).Error("Ingestion failed")
			progressCh <- model.ProgressUpdate{
				Status:    "error",
				Message:   err.Error(),
				Count:     0,
				Completed: true,
			}
		} else {
			progressCh <- model.ProgressUpdate{
				Status:    "success",
				Message:   "Ingestion completed successfully",
				Count:     result.TotalRecords,
				Completed: true,
			}
		}
		close(progressCh)
	}()

	// Stream progress updates to client
	flush := c.Writer.Flush
	for progress := range progressCh {
		// Check if client disconnected
		if c.Request.Context().Err() != nil {
			h.logger.Info("Client disconnected, stopping ingestion")
			cancel()
			return
		}

		// Format as SSE
		data := fmt.Sprintf("data: %s\n\n", progress.ToJSON())
		_, err := fmt.Fprint(c.Writer, data)
		if err != nil {
			h.logger.WithError(err).Error("Failed to write progress update")
			cancel()
			return
		}
		flush()
	}
}