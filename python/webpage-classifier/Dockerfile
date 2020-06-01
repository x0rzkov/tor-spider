FROM python:3-slim

RUN apt-get update && \
    apt-get install -y nano bash jq && \
    pip3 install -U pip

WORKDIR /app
COPY . .

RUN pip3 install -r requirements.txt

CMD ["/bin/bash"]
