# Use the official lightweight Python image
FROM python:3.10-slim

# Set working directory to /app
WORKDIR /app

# Install system dependencies (required for certain compiled packages like pandas/numpy)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements.txt to the working directory
COPY requirements.txt .

# Install dependencies strictly
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose standard port required by Hugging Face Spaces
EXPOSE 7860

# Hugging face will automatically execute this run command:
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7860"]
