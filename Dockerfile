FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=America/Santiago

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY envio_mail_control_efectivo_en_sucursal.py /app/
COPY entrypoint.sh /entrypoint.sh
COPY crontab.txt /etc/cron.d/control-efectivo-cron

RUN sed -i 's/\r$//' /entrypoint.sh \
    && sed -i 's/\r$//' /etc/cron.d/control-efectivo-cron \
    && chmod +x /entrypoint.sh \
    && chmod 0644 /etc/cron.d/control-efectivo-cron \
    && touch /var/log/cron.log

CMD ["/bin/sh", "/entrypoint.sh"]
