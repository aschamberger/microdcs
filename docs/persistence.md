# Persistence

## Persistence Requirements

The dataclasses from the OPC UA Job Mgmt need to be persisted. With a document oriented persistence layer they can be directly stored as is. Schema evolution needs to be possible.

The other requirements are deduplication of cloudevents and passing changed variable state to a single publisher.

## [Redis](https://redis.io/docs/latest/)

Dataclasses are stored to Redis data type JSON. The dataschema is added to an additional field `_dataschema`.
It is derived from the `Config` classes on serialization.

The event data type is used for the OPC UA Job Mgmt implementation to trigger the single instance publisher from the multi instance receiver/event publisher. Events are published directly to MQTT. The state variables however are published with the OPC UA pub/sub mechanism (to not always send unchanged values).

* [Redis course notes](https://medium.com/@krushnakr9/l3-l5-redis-course-3ebc9843925c)
* [Redis Labs Python developer path](https://github.com/redislabs-training/ru-dev-path-py)
* [RedisVL hash vs JSON guide](https://redis.io/docs/latest/develop/ai/redisvl/user_guide/hash_vs_json/) - JSON as we have multi level
* [Redis OM for Python: globally unique primary keys](https://redis.io/blog/introducing-redis-om-for-python/#Globally_unique_primary_keys) - not required as we do not create any new PK