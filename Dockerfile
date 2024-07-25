FROM python:3.12-slim

RUN apt-get update && apt-get upgrade --yes
WORKDIR /app

RUN pip3 install --upgrade pip

ARG VERSION
ENV VERSION=${VERSION:-}

# Install from pip for transparency and simplicity
RUN pip3 install "tiktok_research_api_helper==${VERSION}"

ENTRYPOINT ["tiktok-lib"]
