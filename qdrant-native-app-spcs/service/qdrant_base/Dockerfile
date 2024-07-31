FROM qdrant/qdrant:v1.10.1
RUN apt-get update -y
RUN apt-get install -y python3-pip git net-tools wget curl netcat-traditional uuid-runtime jq
RUN apt-get install -y python3.11-venv
RUN python3 -m venv .venv
RUN . .venv/bin/activate
#COPY qdrant_requirements.txt ./
#RUN .venv/bin/python3 -m pip install -r qdrant_requirements.txt
COPY entrypoint.sh ./