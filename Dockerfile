FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir numpy psutil qdrant-edge-py==0.8.0

COPY exp2_budget.py .
COPY engine ./engine
COPY edge ./edge

CMD ["python", "exp2_budget.py"]
