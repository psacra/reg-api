FROM python:slim

RUN groupadd -g 1012 reg-api && useradd -u 1012 -g 1012 reg-api && pip install --no-cache-dir --upgrade pip && pip3 install --no-cache-dir "fastapi[standard]"
USER reg-api
WORKDIR "/home/reg-api/src"
COPY --chown=reg-api:reg-api ./ /home/reg-api/
ENV PATH="$PATH:/home/reg-api/bin"

ENTRYPOINT ["fastapi","run","--host","0.0.0.0"]
