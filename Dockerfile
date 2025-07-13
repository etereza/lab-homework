# Dockerfile

# Використовуємо офіційний базовий образ Python 3.9-slim
FROM python:3.9-slim

# Встановлюємо робочу директорію всередині контейнера
WORKDIR /app

# Копіюємо файл app.py до робочої директорії
COPY app.py .

# Команда для запуску скрипту
CMD ["python", "app.py"]