VDJServer Tapis
===============

Wrapper functions for accessing the Tapis APIs. The docker container provides
a number of useful utility functions and commands.

Docker Build
============

```
docker build . -t vdjserver/vdj-tapis-js
```

A valid environment file is required to be provided, the following example commands
use the `.env` file in the current directory.

```
alias tapis-js='docker run -v $PWD:/work --env-file .env -it vdjserver/vdj-tapis-js:latest'
```

V3 Meta Load
============

```
tapis-js node v3_meta_load.js /work/file.json tapis_meta
```
