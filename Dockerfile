FROM python:3.7

ENV TZ=Asia/Seoul
WORKDIR /home/kcl
COPY ./ ./
RUN pip install --upgrade pip && pip install -r ./config/requirements.txt
CMD ["python", "app.py"]