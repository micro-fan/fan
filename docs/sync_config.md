# Synchronous services helper

The main goal of the helper to provide integration of synchronous services with the discovery service.
You must use heartbeats to keep connection with zookeeper, so it's required to use separate threads or processes (comments?).
Since wsgi services can be terminated/restarted at any time - it's a better choice to keep the functionality in
a separate process (which functionality?).

Ideally the helper should:

* Read their own config
* Launch a synchronous service
* Check a synchronous service state, let's say calling ping/status methods
* Register service in discovery

Other responsibilities of the helper are config generation from templates, sending information to the discovery service,
 and watching for changes.

## YAML based configuration

Service configuration has to be put into YAML config, which will be read by the helper on service start.

### Syntax

Use `%{VAR_NAME}` to put the environment variables into config. Both keys and values are supported.

Use `<<: *anchor_name` to merge common parts of the configs, like hostname and etc.

Other things, specified by transports, eg. http transport, may use url parameters.

Use `%{LOCAL_IP}` in docker, it will set to internal ip of the docker container.

```yaml
base: &base
  host: '%{HOSTNAME}'
  port: 80

services:
  - name: simple
    version: '1.0.0'
    <<: *base
    methods:
      - name: echo
        url: '/simple/echo'
        method: POST
      - name: status
        url: '/simple/status'
        method: GET
      - name: url_params
        url: '/simple/{id}/'
        method: GET
```
