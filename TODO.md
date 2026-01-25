# TODO

* copilot instrcutions
  * https://github.blog/ai-and-ml/github-copilot/5-tips-for-writing-better-custom-instructions-for-copilot/
  * https://docs.github.com/en/copilot/how-tos/configure-custom-instructions/add-repository-instructions?tool=vscode


* OPC UA Job Spec
  * publish opc ua meta object with retain on app startup for dicovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5


* redis example implementation
  * https://github.com/redislabs-training/ru-dev-path-py

  * schema version?!
  * create index
  * https://redis.io/docs/latest/develop/ai/redisvl/user_guide/hash_vs_json/ >> JSON as we have multi level
  * https://redis.io/blog/introducing-redis-om-for-python/#Globally_unique_primary_keys >> not required as we do not create any new PK

  * https://redis.io/wp-content/uploads/2022/08/8-Data-Modeling-Patterns-in-Redis.pdf
  * https://medium.com/@krushnakr9/l3-l5-redis-course-3ebc9843925c
  * https://university.redis.io/course/hdby8dff3ngc9m?tab=details

* Read the bookmarks on DCS internals
* shutdown handling to unscribe topics and handle existing tasks in current instance before shutdown e.g. on redeploy etc.
* processer config/plugin model: https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312
* add more data model validations?
* sat token change check for MQTT + certs?

* distroless container image python
* Move to aiomqtt v3
  * uv pip install "git+https://github.com/empicano/aiomqtt@v3"
  * https://github.com/empicano/aiomqtt/pull/376#issuecomment-3508036687 >> check puback=0x00 to have subscriber
  * comment on github about missing ssl context parameter in v3 + mqttv5 reauth
  * implement OTEL instrumentation lib
* k8s deployment

* Notes
  * deployment of additional HTTP services container (with e.g. FastAPI) servicing to the outside in same pod or different one with node affinity?
  * what about copying the data n-times and mem consumption + GC impact
  * parallel container instances means always read from redis