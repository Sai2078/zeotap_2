package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Logger middleware adds logging to requests
func Logger(logger *logrus.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := logger.WithTime(c.Request.Time)
		path := c.Request.URL.Path
		
		// Process request
		c.Next()
		
		// Log request
		end := logger.WithFields(logrus.Fields{
			"status":     c.Writer.Status(),
			"method":     c.Request.Method,
			"path":       path,
			"latency":    c.Writer.Header().Get("X-Response-Time"),
			"ip":         c.ClientIP(),
			"user-agent": c.Request.UserAgent(),
		})
		
		// Log based on status code
		if c.Writer.Status() >= 500 {
			end.Error("Server error")
		} else if c.Writer.Status() >= 400 {
			end.Warn("Client error")
		} else {
			end.Info("Request processed")
		}
	}
}