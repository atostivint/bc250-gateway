# bc250-gateway — proxy OpenAI-compatible qui réveille la BC-250 à la demande.
#
# Rien à compiler : application Python pure, dépendances épinglées.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HOME=/var/lib/bc250-gateway

# curl  : utilisé par bc250-plug pour appeler Home Assistant (absent de -slim)
# ssh   : utilisé pour l'arrêt propre et la garde d'occupation sur la carte
#         (absent de -slim ; sans lui le pod démarre mais ne peut jamais dormir)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl openssh-client \
    && rm -rf /var/lib/apt/lists/*

# ssh refuse de tourner sans entrée passwd pour l'UID courant
# (« No user exists for uid 1000 »), même avec HOME positionné : l'utilisateur
# doit exister, pas seulement l'UID.
RUN useradd --uid 1000 --no-create-home --home-dir /var/lib/bc250-gateway \
    --shell /usr/sbin/nologin app

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY gateway.py /app/gateway.py
COPY bc250-plug /usr/local/bin/bc250-plug
RUN chmod 0755 /usr/local/bin/bc250-plug /app/gateway.py

# L'image ne tourne pas en root : le Deployment impose runAsUser 1000.
# L'état (cache /v1/models, drapeau keep-awake, known_hosts ssh) vit sur un
# volume monté, jamais dans l'image.
USER 1000:1000
EXPOSE 8100
ENTRYPOINT ["python", "/app/gateway.py"]