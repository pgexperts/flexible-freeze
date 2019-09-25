FROM python:2-slim

WORKDIR /usr/src/flexible-freeze

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/flexible_freeze.py ./

ENTRYPOINT [ "python", "flexible_freeze.py" ]
