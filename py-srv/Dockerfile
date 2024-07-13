FROM python:3.11.2

COPY bin/ /app

WORKDIR /app

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["python", "app.py"]
