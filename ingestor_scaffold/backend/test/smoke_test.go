package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/router"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestHealthCheck(t *testing.T) {
	// Setup
	gin.SetMode(gin.TestMode)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	
	cfg, err := config.Load()
	assert.NoError(t, err)
	
	r := router.SetupRouter(cfg, logger)
	
	// Create test request
	req, err := http.NewRequest(http.MethodGet, "/health", nil)
	assert.NoError(t, err)
	
	// Create response recorder
	w := httptest.NewRecorder()
	
	// Serve request
	r.ServeHTTP(w, req)
	
	// Check response
	assert.Equal(t, http.StatusOK, w.Code)
	
	// Parse response
	var response map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	
	// Check response body
	assert.Equal(t, "up", response["status"])
}

func TestDiscoverFlatFileSchema(t *testing.T) {
	// Skip if not running in integration test environment
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test")
	}
	
	// Setup
	gin.SetMode(gin.TestMode)
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	
	cfg, err := config.Load()
	assert.NoError(t, err)
	
	r := router.SetupRouter(cfg, logger)
	
	// Create temp CSV file
	tempFile, err := os.CreateTemp("", "test-*.csv")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())
	
	// Write test data
	_, err = tempFile.WriteString("id,name,value\n1,test1,10.5\n2,test2,20.3\n")
	assert.NoError(t, err)
	tempFile.Close()
	
	// Create request body
	requestBody := map[string]string{
		"filePath":  tempFile.Name(),
		"delimiter": ",",
	}
	requestJSON, err := json.Marshal(requestBody)
	assert.NoError(t, err)
	
	// Create test request
	req, err := http.NewRequest(http.MethodPost, "/api/v1/flatfile/schema", bytes.NewBuffer(requestJSON))
	assert.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	
	// Create response recorder
	w := httptest.NewRecorder()
	
	// Serve request
	r.ServeHTTP(w, req)
	
	// Check response
	assert.Equal(t, http.StatusOK, w.Code)
	
	// Parse response
	var response map[string]interface{}
	err = json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	
	// Check response body
	assert.Equal(t, "success", response["status"])
	
	// Check columns
	columns, ok := response["columns"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(columns))
}