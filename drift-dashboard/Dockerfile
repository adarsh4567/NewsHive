FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=100 --retries=5 -r requirements.txt

COPY . .

# Generate the dummy data when the container builds so it's always ready for showcase
RUN pip install --no-cache-dir --default-timeout=100 --retries=5 pyarrow numpy
RUN python generate_dummy_drift_data.py

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
