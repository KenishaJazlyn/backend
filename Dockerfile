FROM python:3.11-slim as builder

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install CPU-only PyTorch (smaller!)
RUN pip install --no-cache-dir --user \
    torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir --user -r requirements.txt

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Copy only installed packages
COPY --from=builder /root/.local /root/.local

# Copy application code
COPY ./app ./app
COPY .env .env

# Set PATH
ENV PATH=/root/.local/bin:$PATH

# Expose port
EXPOSE 8000

# Run app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]