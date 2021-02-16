ARG ARCH=amd64
ARG NODE_VERSION=10
ARG OS=alpine

#### Stage BASE ########################################################################################################
FROM ${ARCH}/node:${NODE_VERSION}-${OS} AS base
USER root
# Copy scripts
COPY scripts/*.sh /tmp/

# Install tools, create Node-RED app and data dir, add user and set rights
RUN set -ex && \
    apk add --no-cache \
        bash \
        tzdata \
        iputils \
        curl \
        nano \
        git \
        openssl \
        openssh-client && \
    mkdir -p /usr/src/node-red /data
 RUN deluser --remove-home node
 RUN adduser -h /usr/src/node-red -D -H node-red -u 1000 
 RUN chown -R node-red:root /data && chmod -R g+rwX /data
 RUN chown -R node-red:root /usr/src/node-red && chmod -R g+rwX /usr/src/node-red
    # chown -R node-red:node-red /data && \
    # chown -R node-red:node-red /usr/src/node-red

# Set work directory
WORKDIR /usr/src/node-red

# package.json contains Node-RED NPM module and node dependencies
COPY package.json .

#### Stage BUILD #######################################################################################################
FROM base AS build

# Install Build tools
RUN apk add --no-cache --virtual buildtools build-base linux-headers udev python && \
    npm install --unsafe-perm --no-update-notifier --no-fund --only=production && \
    /tmp/remove_native_gpio.sh && \
    cp -R node_modules prod_node_modules

#### Stage RELEASE #####################################################################################################
FROM base AS release
ARG BUILD_DATE
ARG BUILD_VERSION
ARG BUILD_REF
ARG NODE_RED_VERSION
ARG ARCH
ARG TAG_SUFFIX=default

LABEL org.label-schema.build-date=${BUILD_DATE} \
    org.label-schema.docker.dockerfile=".docker/Dockerfile.alpine" \
    org.label-schema.license="Apache-2.0" \
    org.label-schema.name="Node-RED" \
    org.label-schema.version=${BUILD_VERSION} \
    org.label-schema.description="Low-code programming for event-driven applications." \
    org.label-schema.url="https://nodered.org" \
    org.label-schema.vcs-ref=${BUILD_REF} \
    org.label-schema.vcs-type="Git" \
    org.label-schema.vcs-url="https://github.com/node-red/node-red-docker" \
    org.label-schema.arch=${ARCH} \
    authors="Dave Conway-Jones, Nick O'Leary, James Thomas, Raymond Mouthaan"

COPY --from=build /usr/src/node-red/prod_node_modules ./node_modules

# Chown, install devtools & Clean up
RUN chown -R node-red:root /usr/src/node-red && \
    /tmp/install_devtools.sh && \
    rm -r /tmp/*

USER node-red

# Env variables
ENV NODE_RED_VERSION=$NODE_RED_VERSION \
    NODE_PATH=/usr/src/node-red/node_modules:/data/node_modules \
    FLOWS=flows.json

# ENV NODE_RED_ENABLE_SAFE_MODE=true    # Uncomment to enable safe start mode (flows not running)
# ENV NODE_RED_ENABLE_PROJECTS=true     # Uncomment to enable projects option

# Expose the listening port of node-red
EXPOSE 1880

# Add a healthcheck (default every 30 secs)
# HEALTHCHECK CMD curl http://localhost:1880/ || exit 1

ENTRYPOINT ["npm", "start", "--cache", "/data/.npm", "--", "--userDir", "/data"]


# Copy package.json to the WORKDIR so npm builds all
# of your added nodes modules for Node-RED
#COPY package.json .
#RUN npm install
USER root
RUN mkdir /rea-data
RUN chown -R node-red:node-red /rea-data && chmod -R g+rwX /rea-data
USER node-red

RUN npm install node-red-dashboard
RUN npm install node-red-contrib-web-worldmap
# Copy _your_ Node-RED project files into place
# NOTE: This will only work if you DO NOT later mount /data as an external volume.
#       If you need to use an external volume for persistence then
#       copy your settings and flows files to that volume instead.

COPY settings.js /data/settings.js
COPY flows_cred.json /data/flows_cred.json
COPY flows.json /data/flows.json


# User configuration directory volume
# VOLUME ["/data"]