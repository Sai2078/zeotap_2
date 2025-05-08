package router

import (
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/handler"
	"github.com/ingestor/internal/middleware"
	"github.com/ingestor/internal/service"
	"github.com/sirupsen/logrus"
)

// SetupRouter configures the router
func SetupRouter(cfg *config.Config, logger *logrus.Logger) *gin.Engine {
	// Create services
	clickhouseService := service.NewClickHouseService(cfg, logger)
	flatFileService := service.NewFlatFileService(cfg, logger)
	ingestService := service.NewIngestService(clickhouseService, flatFileService, cfg, logger)

	// Create handlers
	ingestHandler := handler.NewIngestHandler(clickhouseService, flatFileService, ingestService, cfg, logger)
	joinHandler := handler.NewJoinHandler(clickhouseService, cfg, logger)

	// Create router
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.Logger(logger))
	r.Use(middleware.ErrorHandler())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{cfg.AllowedOrigin},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "up",
		})
	})

	// API v1
	v1 := r.Group("/api/v1")
	{
		// ClickHouse endpoints
		v1.POST("/clickhouse/connect", ingestHandler.ConnectToClickHouse)
		v1.GET("/clickhouse/tables/:tableName/columns", ingestHandler.GetTableColumns)

		// Flat file endpoints
		v1.POST("/flatfile/schema", ingestHandler.DiscoverFlatFileSchema)

		// Preview data
		v1.POST("/preview", ingestHandler.PreviewData)

		// Join preview
		v1.POST("/join/preview", joinHandler.BuildJoinPreview)

		// Ingestion
		v1.POST("/ingest", ingestHandler.StartIngestion)
	}

	return r
}

// SetupServer configures the HTTP server
func SetupServer(r *gin.Engine, cfg *config.Config) *http.Server {
	return &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      r,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  120 * time.Second,
	}
}