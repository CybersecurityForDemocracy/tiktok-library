FROM python:3.12-slim

RUN apt-get update && apt-get upgrade --yes
WORKDIR /app

RUN pip3 install --upgrade pip hatch

COPY README.md pyproject.toml ./
COPY ./src/ ./src/

RUN hatch env create

ENTRYPOINT ["hatch", "run", "tiktok-lib"]
