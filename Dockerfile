FROM node:22-slim AS frontend-build
WORKDIR /ui
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund
COPY frontend ./
RUN npm run build

FROM python:3.12-slim

WORKDIR /app

# Configure pip to be resilient to slow/flaky networks
ENV PIP_DEFAULT_TIMEOUT=300 \
    PIP_RETRIES=10 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .

# Install large/heavy packages first as individual cached layers.
# If any download fails, only that layer needs to be re-fetched on retry.
RUN pip install "qdrant-edge-py==0.8.0"
RUN pip install "opencv-python-headless>=4.8.0"
RUN pip install "qdrant-client==1.19.1"
RUN pip install "numpy>=2.0.0"

# Install remaining lightweight packages all at once
RUN pip install \
    "psutil>=7.0.0" \
    "pytest>=9.0.0" \
    "fastapi>=0.140.0" \
    "pydantic>=2.10.0" \
    "uvicorn>=0.30.0" \
    "httpx>=0.28.0" \
    "python-multipart>=0.0.9"

COPY app ./app
COPY engine ./engine
COPY edge ./edge
COPY memory ./memory
COPY shared ./shared
COPY sync ./sync
COPY server ./server
COPY fixtures ./fixtures
COPY perception ./perception
COPY --from=frontend-build /ui/.output ./frontend/.output
COPY dashboard ./dashboard

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
