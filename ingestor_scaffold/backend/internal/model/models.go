package model

import (
	"encoding/json"
	"time"
)

// Column represents a column in a table
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ClickHouseConnectionParams contains connection parameters for ClickHouse
type ClickHouseConnectionParams struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Token    string `json:"token"`
}

// FlatFileParams contains parameters for flat file operations
type FlatFileParams struct {
	FilePath  string `json:"filePath"`
	Delimiter string `json:"delimiter"`
}

// PreviewParams contains parameters for data preview
type PreviewParams struct {
	SourceType  string    `json:"sourceType"`
	TableName   string    `json:"tableName"`
	FilePath    string    `json:"filePath"`
	Delimiter   string    `json:"delimiter"`
	Columns     []Column  `json:"columns"`
	Query       string    `json:"query,omitempty"`
}

// IngestionParams contains parameters for data ingestion
type IngestionParams struct {
	SourceType     string        `json:"sourceType"`
	TargetType     string        `json:"targetType"`
	TableName      string        `json:"tableName"`
	FlatFileParams FlatFileParams `json:"flatFileParams"`
	Columns        []Column      `json:"columns"`
	Query          string        `json:"query,omitempty"`
}

// JoinTableInfo contains info about a table in a join
type JoinTableInfo struct {
	Name            string   `json:"name"`
	JoinType        string   `json:"joinType,omitempty"`
	JoinCondition   string   `json:"joinCondition,omitempty"`
	SelectedColumns []string `json:"selectedColumns"`
}

// JoinParams contains parameters for join operations
type JoinParams struct {
	Tables      []JoinTableInfo `json:"tables"`
	WhereClause string          `json:"whereClause,omitempty"`
}

// ProgressUpdate represents a progress update during ingestion
type ProgressUpdate struct {
	Status    string `json:"status"`
	Message   string `json:"message"`
	Count     int    `json:"count"`
	Completed bool   `json:"completed"`
}

// ToJSON converts ProgressUpdate to JSON string
func (p ProgressUpdate) ToJSON() string {
	bytes, err := json.Marshal(p)
	if err != nil {
		return `{"status":"error","message":"Failed to marshal progress update"}`
	}
	return string(bytes)
}

// IngestionResult represents the result of an ingestion operation
type IngestionResult struct {
	TotalRecords int `json:"totalRecords"`
}