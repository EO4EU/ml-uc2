FROM python:3.14.0-slim-trixie
ENV FLASK_APP=FuelConsumption.py
RUN apt-get update && apt-get install libsm6 libxext6  -y
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]
