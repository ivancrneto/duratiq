# Build context must be the duratiq project root (../..), so the duratiq package
# is available to install alongside the admin app.
FROM python:3.13-slim

WORKDIR /app

# Install the duratiq package (models + SqlStore) plus the admin's deps.
COPY pyproject.toml README.md /app/duratiq/
COPY src /app/duratiq/src
RUN pip install --no-cache-dir /app/duratiq \
    "fastapi>=0.115" "uvicorn[standard]>=0.30" \
    "pydantic-settings>=2.4" "psycopg[binary]>=3.1"

COPY admin/backend/app /app/app

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
