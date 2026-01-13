# TODO

* Request/Response handling base classes
  * fill source, expiry and subject from runtime_config, ... >> create_event() in processor?
  * register add all childs of union type
  * timer tasks
* MQTTProcessor: sending of outgoing messages which are not responses

Move to aiomqtt v3
* uv pip install "git+https://github.com/empicano/aiomqtt@v3"
* https://github.com/empicano/aiomqtt/pull/376#issuecomment-3508036687 >> check puback=0x00 to have subscriber
* comment on github about missing ssl context parameter in v3

* processer config/plugin model: https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312
* add more data model validations?
* distroless container image python
* Read the bookmarks on DCS internals
* implement OTELInstrumentedMQTTHandler / OTELInstrumentedMessagePackHandler

* OPC UA Job Spec
  * publish opc ua meta object with retain on app startup for dicovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5
  * redis example implementation

* Notes
  * deployment of additional HTTP services container (with e.g. FastAPI) servicing to the outside in same pod or different one with node affinity?
  * what about copying the data n-times and mem consumption + GC impact
  * parallel container instances means always read from redis