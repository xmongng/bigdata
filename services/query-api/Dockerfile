FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY src/ src/
USER 65532:65532
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=4s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=2)"
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
