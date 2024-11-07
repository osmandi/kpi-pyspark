FROM apache/spark-py

USER root

RUN pip install pandas

USER 185