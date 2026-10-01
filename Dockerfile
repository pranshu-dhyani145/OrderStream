FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Run as a non-root user; /data is where the shared SQLite volume is mounted.
RUN useradd --create-home --uid 1000 app && mkdir /data && chown app:app /data
USER app

EXPOSE 8000
# Default = API. The consumer service overrides this with `python -m app.consumer`.
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
