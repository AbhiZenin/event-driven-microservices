FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ARG SERVICE
ENV SERVICE=${SERVICE}
CMD sh -c "uvicorn services.${SERVICE}.main:app --host 0.0.0.0 --port ${PORT:-8000}"
