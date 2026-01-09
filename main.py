import time
import yaml
import logging
import sys
import os
from prometheus_client import start_http_server
from collector import MetricsCollector

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("Main")

def load_config(config_path="config.yaml"):
    if not os.path.exists(config_path):
        logger.error(f"Config file {config_path} not found!")
        sys.exit(1)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    logger.info("Starting SQL Server Metrics Collector...")
    
    config = load_config()
    collection_interval = config.get('collection_interval_seconds', 15)
    export_port = config.get('export_port', 8000)
    
    # Start Prometheus HTTP Server
    logger.info(f"Starting Prometheus Metrics Server on port {export_port}")
    try:
        start_http_server(export_port)
    except Exception as e:
        logger.error(f"Failed to start HTTP server: {e}")
        sys.exit(1)
        
    # Initialize Collector
    collector = MetricsCollector(config)
    
    logger.info(f"Initialization complete. Starting collection loop (Interval: {collection_interval}s)")
    
    # Collection Loop
    try:
        while True:
            start_time = time.time()
            collector.collect()
            elapsed = time.time() - start_time
            logger.info(f"Metrics collected in {elapsed:.2f}s")
            
            sleep_time = max(0, collection_interval - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Stopping collector...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        time.sleep(5) # Wait before retrying loop logic implies restart, but here we just crash or loop? 
                       # Python script usually exits on unhandled exception. 
                       # We catch generic exception inside loop? No, usually safer to let it crash and let supervisor restart, 
                       # but for a simple script, we validly catch it here or just let it exit.
        sys.exit(1)

if __name__ == "__main__":
    main()
