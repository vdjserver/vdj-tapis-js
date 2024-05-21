# Base Image
FROM ubuntu:20.04

LABEL maintainer="VDJServer <vdjserver@utsouthwestern.edu>"

# PROXY: uncomment these if building behind UTSW proxy
#ENV http_proxy 'http://proxy.swmed.edu:3128/'
#ENV https_proxy 'https://proxy.swmed.edu:3128/'
#ENV HTTP_PROXY 'http://proxy.swmed.edu:3128/'
#ENV HTTPS_PROXY 'https://proxy.swmed.edu:3128/'

# Install OS Dependencies
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get install -y --fix-missing \
    make \
    wget \
    xz-utils \
    git \
    wget \
    python3 \
    python3-pip \
    python3-sphinx \
    python3-scipy \
    libyaml-dev \
    curl \
    jq \
    bsdmainutils \
    nano

RUN pip3 install \
    pandas \
    biopython \
    matplotlib \
    airr \
    tapipy \
    python-dotenv

RUN pip3 install --upgrade requests

# node
ENV NODE_VER v18.17.1
RUN wget https://nodejs.org/dist/$NODE_VER/node-$NODE_VER-linux-x64.tar.xz
RUN tar xf node-$NODE_VER-linux-x64.tar.xz
RUN cp -rf /node-$NODE_VER-linux-x64/bin/* /usr/bin
RUN cp -rf /node-$NODE_VER-linux-x64/lib/* /usr/lib
RUN cp -rf /node-$NODE_VER-linux-x64/include/* /usr/include
RUN cp -rf /node-$NODE_VER-linux-x64/share/* /usr/share

# PROXY: More UTSW proxy settings
#RUN npm config set proxy http://proxy.swmed.edu:3128
#RUN npm config set https-proxy http://proxy.swmed.edu:3128
#RUN git config --global http.proxy http://proxy.swmed.edu:3128
#RUN git config --global https.proxy https://proxy.swmed.edu:3128

# Copy project source
RUN mkdir /vdj-tapis-js
COPY . /vdj-tapis-js
RUN cd /vdj-tapis-js && npm install

# ESLint
RUN cd /vdj-tapis-js && npm run eslint

WORKDIR /vdj-tapis-js
