FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY hl_mft ./hl_mft
RUN pip install .

RUN useradd -m -u 1000 bot && mkdir -p /app/data && chown -R bot:bot /app
USER bot
VOLUME ["/app/data"]
EXPOSE 8080 9108

ENTRYPOINT ["hl-mft"]
CMD ["run", "-c", "/app/config.yaml"]
