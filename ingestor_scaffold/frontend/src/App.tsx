import React, { useState } from 'react';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import ConnectionForm from './components/ConnectionForm';
import ColumnSelector from './components/ColumnSelector';
import ProgressBar from './components/ProgressBar';
import JoinBuilder from './components/JoinBuilder';
import { Column, Source, Target, IngestionParams, JoinParams } from './types';
import { previewData, startIngestion } from './api';
import './App.css';

const App: React.FC = () => {
  // State for source/target selection
  const [source, setSource] = useState<Source | null>(null);
  const [target, setTarget] = useState<Target | null>(null);

  // State for connection parameters
  const [clickHouseParams, setClickHouseParams] = useState({
    host: '',
    port: 9440,
    database: '',
    user: '',
    token: '',
  });
  const [flatFileParams, setFlatFileParams] = useState({
    filePath: '',
    delimiter: ',',
  });

  // State for data
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [columns, setColumns] = useState<Column[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Column[]>([]);

  // State for preview
  const [previewVisible, setPreviewVisible] = useState(false);
  const [previewData, setPreviewData] = useState<any[]>([]);

  // State for join (bonus feature)
  const [joinMode, setJoinMode] = useState(false);
  const [joinParams, setJoinParams] = useState<JoinParams>({
    tables: [],
    whereClause: '',
  });
  const [joinQuery, setJoinQuery] = useState('');

  // State for ingestion
  const [ingesting, setIngesting] = useState(false);
  const [progress, setProgress] = useState({
    current: 0,
    total: 0,
    percentage: 0,
    message: '',
  });
  const [completed, setCompleted] = useState(false);

  // Handle source selection
  const handleSourceSelect = (sourceType: Source) => {
    setSource(sourceType);
    
    // Clear any previously selected tables and columns
    setTables([]);
    setSelectedTable('');
    setColumns([]);
    setSelectedColumns([]);
    setJoinMode(false);
  };

  // Handle target selection
  const handleTargetSelect = (targetType: Target) => {
    setTarget(targetType);
  };

  // Handle table selection
  const handleTableSelect = (tableName: string) => {
    setSelectedTable(tableName);
    setSelectedColumns([]);
  };

  // Handle column selection
  const handleColumnSelect = (column: Column, selected: boolean) => {
    if (selected) {
      setSelectedColumns([...selectedColumns, column]);
    } else {
      setSelectedColumns(selectedColumns.filter(col => col.name !== column.name));
    }
  };

  // Handle preview
  const handlePreview = async () => {
    try {
      if (joinMode && joinParams.tables.length > 0) {
        // Preview join data
        const result = await previewData({
          sourceType: 'clickhouse',
          tableName: '',
          query: joinQuery,
          columns: [],
          filePath: '',
          delimiter: '',
        });
        setPreviewData(result.data);
      } else {
        // Preview regular data
        const result = await previewData({
          sourceType: source as string,
          tableName: selectedTable,
          columns: selectedColumns,
          filePath: flatFileParams.filePath,
          delimiter: flatFileParams.delimiter,
        });
        setPreviewData(result.data);
      }
      setPreviewVisible(true);
    } catch (error) {
      toast.error(`Preview failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  // Handle start ingestion
  const handleStartIngestion = async () => {
    try {
      setIngesting(true);
      setCompleted(false);
      setProgress({
        current: 0,
        total: 0,
        percentage: 0,
        message: 'Starting ingestion...',
      });

      // Build ingestion params
      const params: IngestionParams = {
        sourceType: source as string,
        targetType: target as string,
        tableName: selectedTable,
        flatFileParams: flatFileParams,
        columns: selectedColumns,
      };

      // Add join query if in join mode
      if (joinMode && joinQuery) {
        params.query = joinQuery;
      }

      // Start ingestion with SSE for progress updates
      await startIngestion(params, (event) => {
        try {
          const data = JSON.parse(event.data);
          
          if (data.status === 'error') {
            toast.error(`Ingestion error: ${data.message}`);
            setIngesting(false);
            return;
          }
          
          const newProgress = {
            current: data.count,
            total: data.count,
            percentage: 100,
            message: data.message,
          };
          
          setProgress(newProgress);
          
          if (data.completed) {
            setIngesting(false);
            setCompleted(true);
            toast.success(`Ingestion completed! ${data.count} records processed.`);
          }
        } catch (error) {
          console.error('Error parsing SSE data:', error);
        }
      });
    } catch (error) {
      toast.error(`Ingestion failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
      setIngesting(false);
    }
  };

  // Render the appropriate form based on source/target selection
  const renderForm = () => {
    if (!source) {
      return (
        <div className="selection-container">
          <h2>Select Data Source</h2>
          <div className="button-group">
            <button 
              className={`source-button ${source === 'clickhouse' ? 'selected' : ''}`}
              onClick={() => handleSourceSelect('clickhouse')}
            >
              ClickHouse
            </button>
            <button 
              className={`source-button ${source === 'flatfile' ? 'selected' : ''}`}
              onClick={() => handleSourceSelect('flatfile')}
            >
              Flat File
            </button>
          </div>
        </div>
      );
    }

    if (!target) {
      return (
        <div className="selection-container">
          <h2>Select Target</h2>
          <div className="button-group">
            {source === 'clickhouse' && (
              <button 
                className={`target-button ${target === 'flatfile' ? 'selected' : ''}`}
                onClick={() => handleTargetSelect('flatfile')}
              >
                Flat File
              </button>
            )}
            {source === 'flatfile' && (
              <button 
                className={`target-button ${target === 'clickhouse' ? 'selected' : ''}`}
                onClick={() => handleTargetSelect('clickhouse')}
              >
                ClickHouse
              </button>
            )}
          </div>
          <button className="back-button" onClick={() => setSource(null)}>Back</button>
        </div>
      );
    }

    return (
      <div className="connection-container">
        <ConnectionForm
          source={source}
          target={target}
          clickHouseParams={clickHouseParams}
          flatFileParams={flatFileParams}
          setClickHouseParams={setClickHouseParams}
          setFlatFileParams={setFlatFileParams}
          tables={tables}
          setTables={setTables}
          selectedTable={selectedTable}
          setSelectedTable={handleTableSelect}
          setColumns={setColumns}
          joinMode={joinMode}
          setJoinMode={setJoinMode}
        />
        
        {columns.length > 0 && !joinMode && (
          <ColumnSelector
            columns={columns}
            selectedColumns={selectedColumns}
            onSelectColumn={handleColumnSelect}
          />
        )}
        
        {source === 'clickhouse' && joinMode && (
          <JoinBuilder
            tables={tables}
            onJoinParamsChange={setJoinParams}
            onJoinQueryChange={setJoinQuery}
          />
        )}
        
        <div className="action-buttons">
          {(selectedColumns.length > 0 || (joinMode && joinQuery)) && (
            <button 
              className="preview-button" 
              onClick={handlePreview}
              disabled={ingesting}
            >
              Preview Data
            </button>
          )}
          
          {(selectedColumns.length > 0 || (joinMode && joinQuery)) && (
            <button 
              className="ingest-button" 
              onClick={handleStartIngestion}
              disabled={ingesting}
            >
              Start Ingestion
            </button>
          )}
          
          <button className="back-button" onClick={() => setTarget(null)}>Back</button>
        </div>
        
        {ingesting && (
          <ProgressBar 
            current={progress.current} 
            total={progress.total} 
            percentage={progress.percentage} 
            message={progress.message}
          />
        )}
        
        {completed && (
          <div className="completion-message">
            <h3>Ingestion Completed!</h3>
            <p>Total records processed: {progress.current}</p>
          </div>
        )}
        
        {previewVisible && previewData.length > 0 && (
          <div className="preview-container">
            <h3>Data Preview (First {previewData.length} rows)</h3>
            <div className="table-container">
              <table>
                <thead>
                  <tr>
                    {Object.keys(previewData[0]).map(key => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.map((row, rowIndex) => (
                    <tr key={rowIndex}>
                      {Object.values(row).map((cell, cellIndex) => (
                        <td key={cellIndex}>{cell?.toString() || ''}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <button 
              className="close-preview-button" 
              onClick={() => setPreviewVisible(false)}
            >
              Close Preview
            </button>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>Ingestor</h1>
        <p>Bidirectional ClickHouse & Flat File Data Ingestion Tool</p>
      </header>
      <main className="app-main">
        {renderForm()}
      </main>
      <footer className="app-footer">
        <p>&copy; 2025 Ingestor - ClickHouse & Flat File Data Integration</p>
      </footer>
      <ToastContainer position="bottom-right" />
    </div>
  );
};

export default App;