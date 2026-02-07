FROM python:3.10-slim

# Install FFmpeg only (no complex dependencies)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install minimal Python dependencies
RUN pip install --no-cache-dir \
    runpod>=1.3.0 \
    requests>=2.28.0

# Copy handler
WORKDIR /app
COPY handler.py /app/handler.py

# RunPod serverless entrypoint
CMD ["python", "-u", "/app/handler.py"]
