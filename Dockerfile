# Multi-stage / lightweight python base
FROM python:3.11-slim

# Prevent python from buffering stdout/stderr and writing .pyc files
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

# Set work directory
WORKDIR /app

# Install dependencies first for Docker caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY workload_app.py .

# Create a non-root user for security best practices
RUN useradd -m -u 10001 appuser && chown -R appuser:appuser /app
USER appuser

# Expose the application port
EXPOSE 5000

# Health check to ensure the service is running and responsive
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request, os; p = os.environ.get('PORT', '5000'); urllib.request.urlopen(f'http://localhost:{p}/api/stats').read()" || exit 1

# Start the application
CMD ["python", "workload_app.py", "5000"]
