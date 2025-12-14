# ============================================
# CLOUD COMPUTING - Docker Container
# ============================================
FROM python:3.9-slim

WORKDIR /app

# Copy source code
COPY src/ /app/
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create directories for DFS and logs
RUN mkdir -p /app/cache_data /app/logs

# Default command (can be overridden)
CMD ["python", "rpc_proxy_server.py"]

