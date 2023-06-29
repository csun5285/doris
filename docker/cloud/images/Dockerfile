# The image for both compile and runtime
FROM ubuntu:22.04 as selectdb-base

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" \
    CLOUD_MODE=1 \
    REPOSITORY_URL="https://doris-thirdparty-repo.bj.bcebos.com/thirdparty"

RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates

# softwares for building
RUN apt-get install -y build-essential maven byacc flex automake libtool-bin \
        bison binutils-dev libiberty-dev zip unzip libncurses5-dev curl git ninja-build \
        python-is-python3 python3 autopoint pkg-config autoconf automake libtool \
        openjdk-8-jre openjdk-8-jdk cmake ccache default-mysql-client wget

# softwares for running & monitor
RUN apt-get install -y patchelf less vim htop iproute2 \
        numactl tar jq iotop sysstat smartmontools tcpdump \
        iputils-ping dnsutils

# The image contains build output
FROM selectdb-base as selectdb-output

ARG SELECTDB_CORE_URL=
ARG USE_AVX2=0

ADD ${SELECTDB_CORE_URL} /opt/selectdb/selectdb-core

RUN cd /opt/selectdb/selectdb-core && \
    USE_AVX2=${USE_AVX2} bash build.sh --fe --be --cloud && \
    bash build.sh --clean && \
    rm -rf ./cloud/build_*/ && \
    rm -rf ./thirdparty/installed/ ./thirdparty/src/

# The image for fe
FROM selectdb-base as selectdb-fe

COPY --from=selectdb-output /opt/selectdb/selectdb-core/output/fe/ /opt/selectdb/fe

WORKDIR /opt/selectdb/fe

# It allows users to provide custom config via mount overrides.
RUN mkdir -p /opt/selectdb/fe/custom_conf/ && \
    echo 'custom_config_dir=${DORIS_HOME}/custom_conf/' >> /opt/selectdb/fe/conf/fe.conf

ENTRYPOINT ["/opt/selectdb/fe/bin/start_fe.sh"]

# The image for be
FROM selectdb-base as selectdb-be

COPY --from=selectdb-output /opt/selectdb/selectdb-core/output/be/ /opt/selectdb/be

WORKDIR /opt/selectdb/be

# It allows users to provide custom config via mount overrides.
RUN mkdir -p /opt/selectdb/be/custom_conf/ && \
    echo 'custom_config_dir=${DORIS_HOME}/custom_conf/' >> /opt/selectdb/be/conf/be.conf

ENTRYPOINT ["/opt/selectdb/be/bin/start_be.sh"]

# The image for ms
FROM selectdb-base as selectdb-ms

COPY --from=selectdb-output /opt/selectdb/selectdb-core/cloud/output/ /opt/selectdb/meta-service

WORKDIR /opt/selectdb/meta-service

ENTRYPOINT ["/opt/selectdb/meta-service/bin/start.sh", "--meta-service"]

# The image for recycler
FROM selectdb-ms as selectdb-recycler

RUN mv /opt/selectdb/meta-service /opt/selectdb/recycler

WORKDIR /opt/selectdb/recycler

ENTRYPOINT ["/opt/selectdb/recycler/bin/start.sh", "--recycler"]

