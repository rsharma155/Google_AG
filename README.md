# SQL Server Metrics Collector

A lightweight, Python-based metrics collector for SQL Server that exposes metrics for Prometheus and Grafana.

## Features
- **Lightweight**: Optimized DMV queries.
- **Comprehensive**: CPU, Memory, I/O, Waits, Active Sessions, Blocking, Failed Jobs.
- **Standard**: Exposes metrics on `/metrics` endpoint compatible with Prometheus.

## Prerequisites
1. **ODBC Driver for SQL Server**:
   - **MacOS**: `brew install msodbcsql18`
   - **Ubuntu/Debian**: `sudo apt-get install msodbcsql18`
   - **Windows**: Download "ODBC Driver 18 for SQL Server" from Microsoft.
2. **Network Access**: Ensure the machine running this app can connect to the SQL Server port (default 1433).

## Configuration
Edit `config.yaml`:
```yaml
server: "192.168.1.10,1433"      # IP,Port (Comma separated preferred)
database: "master"
username: "sa"
password: "your_password"
driver: "{ODBC Driver 18 for SQL Server}"
encrypt: "no"                   # "yes" if server supports encryption, "no" otherwise
collection_interval_seconds: 15
export_port: 8000
```

## Running the Application

### Option A: Using Python (Source)
1. Install Python 3.9+.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run:
   ```bash
   python main.py
   ```

### Option B: Using Executable (No Python Required)
1. Build the executable (on a machine with Python):
   ```bash
   sh build_executable.sh
   ```
2. Copy `dist/sql_metrics_collector` and `config.yaml` to the target server.
3. Run:
   ```bash
   ./sql_metrics_collector
   ```

## Visualization
Import `grafana_dashboard.json` into Grafana to visualize the metrics.

## How to Add More Metrics

1. **Add the SQL Query**:
   Open `queries.py` and add a new constant variable with your SQL query.
   ```python
   GET_BUFFER_CACHE_HIT_RATIO = """
   SELECT object_name, counter_name, cntr_value 
   FROM sys.dm_os_performance_counters 
   WHERE counter_name = 'Buffer cache hit ratio';
   """
   ```

2. **Define the Metric**:
   Open `collector.py` and define a new Gauge or Counter at the top.
   ```python
   SQL_BUFFER_CACHE = Gauge('sql_buffer_cache_hit_ratio', 'Buffer Cache Hit Ratio')
   ```

3. **Add Collection Logic**:
   In `collector.py`, add a new method to `MetricsCollector` class.
   ```python
   def _collect_buffer_cache(self, cursor):
       cursor.execute(GET_BUFFER_CACHE_HIT_RATIO)
       row = cursor.fetchone()
       if row:
           SQL_BUFFER_CACHE.set(row.cntr_value)
   ```

4. **Register the Collector**:
   Add `self._collect_buffer_cache(cursor)` to the `collect` method in `MetricsCollector` class.

