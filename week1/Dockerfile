FROM python:3.9

COPY requirements.txt .

run apt-get install wget
RUN pip install -r requirements.txt

WORKDIR /app
COPY docker_import.py .

# CMD ["python", "hello.py", "30"]
ENTRYPOINT ["python", "docker_import.py"]
