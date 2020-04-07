FROM tiangolo/uwsgi-nginx-flask:python3.7	

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

ENV UWSGI_INI ./uwsgi.ini

COPY . /app
