FROM python:3.10-slim

# Install system dependencies for MoviePy/FFmpeg
RUN apt-get update && apt-get install -y \
    ffmpeg \
    libsm6 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    runpod>=1.3.0 \
    moviepy==1.0.3 \
    requests>=2.28.0 \
    numpy \
    imageio-ffmpeg

# Copy handler
WORKDIR /app
COPY handler.py /app/handler.py

# RunPod serverless entrypoint
CMD ["python", "-u", "/app/handler.py"]
