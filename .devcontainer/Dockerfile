FROM python:3.12-slim-bookworm

RUN apt update \
    && apt install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/bin/bash", "-c"]
