FROM python:3.9

COPY requirements.txt .

RUN pip install -r requirements.txt

WORKDIR /app
COPY hello.py .

CMD ["python", "hello.py", "30"]
# ENTRYPOINT ["bash"]
