# syntax=docker/dockerfile:1.7

FROM apache/iceberg-rest-fixture:1.10.1@sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f

USER root
RUN install -d -o iceberg -g iceberg -m 0750 /var/lib/iceberg
USER iceberg:iceberg
