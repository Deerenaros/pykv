FROM python:3.11

WORKDIR /app/

ADD requirements.txt ./
RUN pip install -r requirements.txt
ADD . ./

ENV PYTHONPATH=.:pykv

CMD ["python", "-m", "pykv"]