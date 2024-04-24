FROM node:12-bullseye-slim

LABEL maintainer="VDJServer <vdjserver@utsouthwestern.edu>"

# PROXY: uncomment these if building behind UTSW proxy
#ENV http_proxy 'http://proxy.swmed.edu:3128/'
#ENV https_proxy 'https://proxy.swmed.edu:3128/'
#ENV HTTP_PROXY 'http://proxy.swmed.edu:3128/'
#ENV HTTPS_PROXY 'https://proxy.swmed.edu:3128/'

# PROXY: More UTSW proxy settings
#RUN npm config set proxy http://proxy.swmed.edu:3128
#RUN npm config set https-proxy http://proxy.swmed.edu:3128
#RUN git config --global http.proxy http://proxy.swmed.edu:3128
#RUN git config --global https.proxy https://proxy.swmed.edu:3128

# Install OS Dependencies
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get install -y --fix-missing \
    make \
    wget \
    xz-utils \
    git \
    wget

# Copy project source
RUN mkdir /vdj-tapis-js
COPY . /vdj-tapis-js
RUN cd /vdj-tapis-js && npm install

# ESLint
RUN cd /vdj-tapis-js && npm run eslint

WORKDIR /vdj-tapis-js
