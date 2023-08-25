FROM python:3.11
LABEL authors="JackLewis"

RUN apt-get update

COPY main.py /home/main.py
COPY /app /home/app

COPY minimalRequirements.txt ./

RUN pip install --no-cache-dir --upgrade pip \
  && pip install --no-cache-dir -r minimalRequirements.txt

CMD ["/home/main.py"]
ENTRYPOINT ["python"]