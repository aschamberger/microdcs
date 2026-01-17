# TODO

* Read the bookmarks on DCS internals
* OPC UA Job Spec
  * publish opc ua meta object with retain on app startup for dicovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5
  * redis example implementation
* shutdown handling to unscribe topics and handle existing tasks in current instance before shutdown e.g. on redeploy etc.
* processer config/plugin model: https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312
* add more data model validations?

* distroless container image python
* Move to aiomqtt v3
  * uv pip install "git+https://github.com/empicano/aiomqtt@v3"
  * https://github.com/empicano/aiomqtt/pull/376#issuecomment-3508036687 >> check puback=0x00 to have subscriber
  * comment on github about missing ssl context parameter in v3
  * implement OTEL instrumentation lib
* k8s deployment

* Notes
  * deployment of additional HTTP services container (with e.g. FastAPI) servicing to the outside in same pod or different one with node affinity?
  * what about copying the data n-times and mem consumption + GC impact
  * parallel container instances means always read from redis