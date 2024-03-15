FROM python

RUN pip install poetry
RUN apt-get update && apt-get install -y libfuse-dev

WORKDIR /app
RUN poetry config virtualenvs.path .venv
COPY pyproject.toml poetry.lock /app/
RUN poetry install

COPY . /app

RUN mkdir -p /mnt/pgfs

CMD poetry install && poetry run pgfs /mnt/pgfs