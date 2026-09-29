FROM node:22-slim AS dashboard-build
WORKDIR /ui
COPY dashboard/package*.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard ./
RUN npm run build

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY engine ./engine
COPY edge ./edge
COPY memory ./memory
COPY shared ./shared
COPY sync ./sync
COPY server ./server
COPY fixtures ./fixtures
COPY perception ./perception
COPY --from=dashboard-build /ui/dist ./dashboard/dist

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
