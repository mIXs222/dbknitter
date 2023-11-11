FROM redis:7.2.2-bookworm

RUN apt update && apt -y upgrade
RUN apt install -y python3 python3-pip
RUN apt install -y build-essential libssl-dev libffi-dev python3-dev
RUN apt install -y python3.11-venv

RUN python3 -m venv /venv && . /venv/bin/activate && pip install pandas redis
