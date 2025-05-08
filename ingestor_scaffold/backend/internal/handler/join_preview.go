package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/model"
	"github.com/ingestor/internal/service"
	"github.com/sirupsen/logrus"
)

// JoinHandler handles the join functionality
type JoinHandler struct {
	clickhouseService service.ClickHouseService
	cfg               *config.Config
	logger            *logrus.Logger
}

// NewJoinHandler creates a new join handler
func NewJoinHandler(
	clickhouseService service.ClickHouseService,
	cfg *config.Config,
	logger *logrus.Logger,
) *JoinHandler {
	return &JoinHandler{
		clickhouseService: clickhouseService,
		cfg:               cfg,
		logger:            logger,
	}
}

// BuildJoinPreview builds a preview of the join query
func (h *JoinHandler) BuildJoinPreview(c *gin.Context) {
	var params model.JoinParams
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

	// Build query
	query, err := h.clickhouseService.BuildJoinQuery(params)
	if err != nil {
		h.logger.WithError(err).Error("Failed to build join query")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to build join query: " + err.Error(),
		})
		return
	}

	// Get all selected columns
	columns := make([]string, 0)
	for _, table := range params.Tables {
		for _, col := range table.SelectedColumns {
			columns = append(columns, col)
		}
	}

	// Preview data
	data, err := h.clickhouseService.ExecuteJoinPreview(ctx, query, h.cfg.MaxPreviewRows)
	if err != nil {
		h.logger.WithError(err).Error("Failed to execute join preview")
		c.JSON(http.StatusInternalServerError, gin.H{
			"status":  "error",
			"message": "Failed to execute join preview: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"query":  query,
		"data":   data,
		"count":  len(data),
	})
}