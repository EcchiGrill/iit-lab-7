FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY meet_consumer_bot.py .

CMD ["python", "meet_consumer_bot.py"]