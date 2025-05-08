# Ingestor - Bidirectional ClickHouse & Flat File Data Ingestion Tool

## Architecture Overview

```mermaid
graph TD
    User[User] --> |Interacts with| Frontend[React Frontend]
    Frontend --> |REST API calls| Backend[Go Backend API]
    Frontend <--> |SSE Progress Events| Backend
    Backend --> |JWT Auth & SQL Queries| ClickHouse[ClickHouse DB]
    Backend <--> |Read/Write| FlatFile[Flat Files CSV/TSV]
    
    subgraph "Frontend Components"
        ConnectionForm[Connection Form]
        ColumnSelector[Column Selector] 
        ProgressBar[Progress Bar]
        JoinBuilder[Join Builder]
        DataPreview[Data Preview]
    end
    
    subgraph "Backend Services"
        ClickHouseService[ClickHouse Service]
        FlatFileService[Flat File Service]
        IngestService[Ingestion Service]
    end