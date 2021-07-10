FROM abelbarrera15/spark:latest

WORKDIR /app

COPY . /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

EXPOSE 5000

# CMD ["spark-shell"]

CMD ["flask", "run", "--host=0.0.0.0"]
# CMD ["api.py"]