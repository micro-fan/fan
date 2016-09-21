# Syncronous services helper

Main goal for this helper is providing integration with discovery service for syncronous services. You cannot keep connections with zookeeper without heartbeats, so you need separate thread or process. Since wsgi services are can be terminated/restarted at any moment better choice is keep this functionality in separate process.

Ideally this helper should:

* Read own config
* Launch syncronous service
* Check syncronous service state, eg. by calling ping/status methods
* Register service in discovery

Other part of responsibilities of this helper is config generation from template + discovery information and watching for changes.

## YAML based configuration

First of all we're going to support yaml configuration for sync helper.

This is just stub to register service in discovery, so you need to provide full information to be written to discovery. Rather tedious, but works fine as start point

### Syntax

You may use `%{VAR_NAME}` to put environmental variables in config. Both keys and values are supported.

You may use `<<: *anchor_name` to merge common parts into config, like hostname and etc.

Other things specified by transports, eg. http transport may use passed parameters in url.

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
