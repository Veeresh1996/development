FROM mysql:5.7

RUN DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-setuptools python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    mysql-connector-python==8.0.18 \
    smart_open==1.9.0 \
    s3fs==0.2.2

WORKDIR /app
CMD [ "/usr/bin/python3", "main.py" ]
COPY app /app
