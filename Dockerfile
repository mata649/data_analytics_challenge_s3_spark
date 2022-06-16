FROM datamechanics/spark:3.2.1-hadoop-3.3.1-java-11-scala-2.12-python-3.8-dm17

WORKDIR /opt/application/

ENV PYSPARK_MAJOR_PYTHON_VERSION = 3

RUN wget https://jdbc.postgresql.org/download/postgresql-42.4.0.jar -P /opt/spark/jars

COPY requeriments.txt .

RUN pip install -r requeriments.txt

COPY src/ src/

COPY .env .


