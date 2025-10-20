FROM python:3.11-slim AS builder
WORKDIR /app
COPY requirements.txt ./
RUN pip install --upgrade pip && pip wheel --wheel-dir=/wheels -r requirements.txt

FROM python:3.11-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    TZ=Asia/Jakarta
WORKDIR /app
COPY --from=builder /wheels /wheels
COPY requirements.txt ./
RUN pip install --no-cache-dir --find-links=/wheels -r requirements.txt && rm -rf /wheels
COPY . .
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
