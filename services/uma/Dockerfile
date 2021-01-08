FROM python:3.8-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME

COPY requirements.txt .
COPY main.py .

RUN apt-get update && apt-get install -y libssl-dev 
RUN apt-get install -y gcc
RUN pip install -r requirements.txt

EXPOSE 8080
ENV PORT 8080

ENTRYPOINT ["python"]
CMD ["main.py"]
