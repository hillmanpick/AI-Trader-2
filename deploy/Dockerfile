FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY service/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY service /app/service
COPY skills /app/skills
RUN useradd --uid 10001 --create-home trader && mkdir -p /data /app/service/server/logs && chown -R trader:trader /data /app/service/server/logs
USER trader
WORKDIR /app/service/server
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000","--workers","1"]
