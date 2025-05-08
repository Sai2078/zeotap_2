package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// ErrorHandler handles errors in the API
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Check for errors
		if len(c.Errors) > 0 {
			// Return the first error
			err := c.Errors.Last()
			
			// Default to internal server error
			code := http.StatusInternalServerError
			
			// Check for specific error types
			switch err.Type {
			case gin.ErrorTypeBind:
				code = http.StatusBadRequest
			case gin.ErrorTypePublic:
				code = http.StatusBadRequest
			}
			
			// Send error response
			c.JSON(code, gin.H{
				"status":  "error",
				"message": err.Error(),
			})
			
			// Stop processing
			c.Abort()
		}
	}
}