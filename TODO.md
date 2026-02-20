# TODO

* OPC UA Job Spec Publisher (single instance)
  * publish opc ua meta object with retain on app startup for discovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5

* expiration handling >> handle_expiration() in processors
* shutdown handling to unscribe topics and handle existing tasks in current instance before shutdown e.g. on redeploy etc.

* Move to aiomqtt v3
  * uv pip install "git+https://github.com/empicano/aiomqtt@v3"
  * https://github.com/empicano/aiomqtt/pull/376#issuecomment-3508036687 >> check puback=0x00 to have subscriber
  * sat token + cert change check when v3 gets ssl + reauth features
* implement OTEL instrumentation lib
* k8s deployment

* Notes
  * Read the bookmarks on DCS internals
  * copilot instructions
    * https://github.blog/ai-and-ml/github-copilot/5-tips-for-writing-better-custom-instructions-for-copilot/
    * https://docs.github.com/en/copilot/how-tos/configure-custom-instructions/add-repository-instructions?tool=vscode
  * deployment of additional HTTP services container (with e.g. FastAPI) servicing to the outside in same pod or different one with node affinity?
