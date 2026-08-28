# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.8.15@sha256:a5727064a0de127bdb7c9d3c1383f3a9ac307d9f2d8a391edc7896c54289ced0 AS uv

FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu@sha256:9b0a6c2c860f5e7d18dd5270286fef32a09c7f5a7e2b0dbe5642de8a3a02ab3e AS python-builder

ARG PYTHON_VERSION=3.12.13
ARG PYTHON_SOURCE_SHA256=0816c4761c97ecdb3f50a3924de0a93fd78cb63ee8e6c04201ddfaedca500b0b

USER root
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential libbz2-dev libffi-dev liblzma-dev libreadline-dev libsqlite3-dev \
      libssl-dev tk-dev uuid-dev zlib1g-dev; \
    rm -rf /var/lib/apt/lists/*; \
    wget -q -O /tmp/Python.tgz \
      "https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz"; \
    echo "${PYTHON_SOURCE_SHA256}  /tmp/Python.tgz" | sha256sum -c -; \
    mkdir /tmp/python-source; \
    tar -xzf /tmp/Python.tgz --strip-components=1 -C /tmp/python-source; \
    cd /tmp/python-source; \
    ./configure --prefix=/opt/cpython --with-ensurepip=install; \
    make -j"$(nproc)"; \
    make install

FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu@sha256:9b0a6c2c860f5e7d18dd5270286fef32a09c7f5a7e2b0dbe5642de8a3a02ab3e

ARG COMET_VERSION=1.0.0
ARG COMET_SHA256=fc47c35419c20c38743ad5752ae3ae32e7a167923be628878d9caa22f41d6b67
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_RUNTIME_SHA256=d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242
ARG ICEBERG_AWS_SHA256=38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f

USER root
COPY --from=uv /uv /uvx /usr/local/bin/
COPY --from=python-builder /opt/cpython /opt/cpython
COPY infrastructure/docker/download-verified.sh /usr/local/bin/download-verified.sh

RUN sh /usr/local/bin/download-verified.sh \
      /opt/spark/jars/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar \
      "${COMET_SHA256}" \
      "https://repo.maven.apache.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/${COMET_VERSION}/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/datafusion/comet-spark-spark4.1_2.13/${COMET_VERSION}/comet-spark-spark4.1_2.13-${COMET_VERSION}.jar"

RUN sh /usr/local/bin/download-verified.sh \
      /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar \
      "${ICEBERG_RUNTIME_SHA256}" \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar"

RUN sh /usr/local/bin/download-verified.sh \
      /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "${ICEBERG_AWS_SHA256}" \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

ENV UV_PROJECT_ENVIRONMENT=/opt/lakehouse/.venv \
    UV_PYTHON=/opt/cpython/bin/python3.12 \
    UV_PYTHON_DOWNLOADS=never \
    UV_HTTP_TIMEOUT=120 \
    UV_LINK_MODE=copy

WORKDIR /opt/lakehouse
COPY pyproject.toml uv.lock runtime-versions.lock /opt/lakehouse/

# pyproject.toml names README.md as project metadata. A placeholder lets the dependency-only sync
# stay cached when documentation changes; the real files are copied before the editable install.
# Docker ADD verifies and caches the pinned PyArrow wheel before uv installs it locally. This
# avoids uv's large-wheel HTTP extraction timeout under Docker Desktop while preserving the lock.
RUN set -eux; \
    touch README.md; \
    uv sync --frozen --no-dev --no-install-project --no-install-package pyarrow \
      --python /opt/cpython/bin/python3.12

ADD --checksum=sha256:b7ae0bbdc8c6674259b25bef5d2a1d6af5d39d7200c819cf99e07f7dfef1c51e \
    https://files.pythonhosted.org/packages/ad/90/2660332eeb31303c13b653ea566a9918484b6e4d6b9d2d46879a33ab0622/pyarrow-21.0.0-cp312-cp312-manylinux_2_28_x86_64.whl \
    /tmp/pyarrow-21.0.0-cp312-cp312-manylinux_2_28_x86_64.whl

RUN uv pip install --python /opt/lakehouse/.venv/bin/python --no-deps \
      /tmp/pyarrow-21.0.0-cp312-cp312-manylinux_2_28_x86_64.whl \
    && rm -f /tmp/pyarrow-21.0.0-cp312-cp312-manylinux_2_28_x86_64.whl

COPY README.md LICENSE /opt/lakehouse/
COPY analysis /opt/lakehouse/analysis
COPY benchmark /opt/lakehouse/benchmark
COPY data /opt/lakehouse/data
COPY pipeline /opt/lakehouse/pipeline
COPY scripts /opt/lakehouse/scripts
COPY workloads /opt/lakehouse/workloads
COPY infrastructure/spark /opt/lakehouse/infrastructure/spark

RUN set -eux; \
    uv sync --frozen --no-dev --python /opt/cpython/bin/python3.12; \
    chown -R spark:spark /opt/lakehouse

ENV PATH=/opt/lakehouse/.venv/bin:${PATH} \
    PYSPARK_PYTHON=/opt/lakehouse/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/lakehouse/.venv/bin/python \
    SPARK_CONF_DIR=/opt/lakehouse/infrastructure/spark

WORKDIR /opt/lakehouse
USER spark
