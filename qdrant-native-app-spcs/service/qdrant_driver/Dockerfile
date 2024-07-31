#FROM nvidia/cuda:12.1.0-cudnn8-devel-ubuntu22.04
FROM nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04
RUN apt-get update -y
RUN apt-get -y install gcc
RUN apt-get install -y python3-pip git net-tools wget gcc curl jq unzip
RUN pip3 install --upgrade pip
COPY usecase_requirements.txt ./
RUN pip3 install -r usecase_requirements.txt
COPY entrypoint.sh ./
ENV PYTHONUNBUFFERED=1
RUN pip3 uninstall -y  transformer-engine
RUN mkdir -p ./app/
COPY ./app/config.py ./config.py
COPY ./app/search-fastembed.py ./search-fastembed.py
COPY ./app/search-snowflakeembed.py ./search-snowflakeembed.py
ENTRYPOINT ["./entrypoint.sh"]