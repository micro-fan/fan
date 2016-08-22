# Use cases list

## Service update strategies
* Atomic replace service with service + Distributed
* Proxy service
* Routing: support version
* Rollback / switch version button
* Complex deps between components

## Discovery usecases
* unique endpoint id

### Config usecases
* get config
* update config
* subscribe for update with companion service

## Versioning usecases
* global version for huge web endpoints collection or version per endpoint? hard to track all version changes
* multiple versions on some time
* how to indicate that service missing concrete version of other service

## Sharding usecases
* simple sharding param
* complex sharding: /store/{shard1}/item/{shard2}/
* versioning?

## Orchestration usecases
* swarm orchestration
* require orchestration master -> need config befor run orchestration tool
* separate scheduler with deps support

## Context usecases
* tracing.span passing into internal calls
* logging with span, how to handle logging in libraries without log support? global logger redefinition

## Local usecases
* don't pay for invocation / easy flow
* don't pay for dynamic features (version updates)

## Queue migration to other service


# Tech features

## Zk things
* Zk watch - enable cheap watching, but not consistent until vnode register as service user
endpoint/{state}/
endpoint/{state}/subscribers/ -> store vnode with user info, uniq discovery id?
* Zk sequence - as unique id generator


# Basics

## Message format
 * tracing headers
 * message headers (ttl)
 * payload


## API
Usage:
 * rpc (timeout)
 * push

Services:
 * register
 * subscribe / unsubscribe (queue-like)

## Layers
* Application (global process)
* Service
* Transport
* Tracing
* Metrics
* Circuit breaker
* Configs

## Discovery and Configs
* find service
* register
* config get / subscribe
* config update


## Service description
* name
* type (rpc, queue)
* route type: http, amqp, redis, database, local?
* route persistance: persistent, dynamic
* sharding
* unique

ProxyService

## Application
Encapsulate discovery, configs.
We can create multiple applications for testing purposes.

Usually will be one application on process level.

## Transport
ProxyEndpoint <-transport-> LocalEndpoint : Service

* http
* amqp
* redis
* IPC/local

## Testing
* tcurl analogue
* mock for logging, tracer, metrics, discovery, config

## Utils
* http<->amqp relay (relay/host/vhost/)
* logging
* metrics
* tracing
* tipsi pypi

## TODO
* dependency management + versions

# Fantasy services

## Sync worker
* Supports db configuration via django admin site
* Separate docker container
* Separate codebase

```python
class AppRoutes(kit.Routes):
    deployment = '{TIPSI_CONFIG}'
    routes = {
        'lightspeed.config_update': {'type': 'redis'},
        'lightspeed.config_update': ConfigDiscovery['lightspeed_config'] or DefaultConfig,
    }


class LightSpeedSync(Service):
    service_name = 'lightspeed'
    depend = {
        'hard': ['integration_api'],  # we cannot work without this service
        'soft': ['lightspeed'],  # we notify lightspeed, but can work without it
    }

    def serviceStart(self): pass
    def serviceStop(self): pass

    @subscribe('config_update')
    def config_update(self):
        new_config = {}
        # push create forward span
        # usage
        self.rpc.lightspeed.config_update.push(new_config)
        self.rpc[LIGHTSPEED]config_update.push(new_config)

    @rpc('ping')
    def ping(self):
        # after return
        return 'pong'

    @rpc('barcode_sync_clear')
    def run_sync(self, barcodes):
        barcodes = []
        # discover integration api
        # new span -> 'integration_api'
        # check circuit breaker / open span/ perform call / get response / close span
        # return result
        self.rpc.integration_api.barcode_sync_clear(barcodes)

class BarcodeSyncClear(GenericAPIView, DRFService):
    service_name = ['integrations_api', 'barcode_sync_clear']

    @rpc() # can call just barcode_sync_clear()
    def patch(self, request, *args, **kwargs):
        pass

class PublicWineViewset(ModelViewSet, DRFService):
    '''
    DRF API's should pass:
     * user_id

    retail.store[store_id].public_wines.get(inventory_id)

    TODO: url can store params for HTTP api_version, store_id, inventory_id
    '''
    service_name = 'public_wines'

    @rpc('create')  # item parameters
    def create(self, request, *a, **k): pass

    @rpc('list')  # filters
    def list(self, request, *a, **k): pass

    @rpc('get')  # id
    def retrieve(self, request, *a, **k): pass

    @rpc('update')  # id
    def update(self, request, *a, **k): pass

    @rpc('delete')  # id
    def destroy(self, request, *a, **k): pass

```
