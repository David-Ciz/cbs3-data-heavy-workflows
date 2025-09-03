FROM apache/airflow:2.10.4-python3.11
USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y git make g++ gfortran libnetcdf-dev
RUN git clone https://github.com/Amber-MD/cpptraj.git /tmp/cpptraj
RUN cd /tmp/cpptraj && ./configure --buildlibs gnu && make -j4 install

COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
