FROM jupyter/all-spark-notebook

COPY EDA.ipynb /opt/
COPY data-dump /opt/

WORKDIR /opt 