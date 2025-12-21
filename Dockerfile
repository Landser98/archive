FROM python:3.12-slim
RUN apt-get update && apt-get install -y \
    ghostscript \
    python3-tk \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir $(grep -v "pipreqs" requirements.txt) && \
    pip install --no-cache-dir uvicorn fastapi python-multipart

COPY . .

EXPOSE 8000

CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]