From python:latest
COPY . /python
WORKDIR /python
RUN pip install -r requirements.txt

COPY object_definition.json /python/weatherPreparationLaura/object_definition.json

ENTRYPOINT ["python3"]

