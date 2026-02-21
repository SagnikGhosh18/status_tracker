FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY status_tracker.py .

CMD ["python", "-u", "status_tracker.py"]
