# Persistence

MicroDCS uses Redis to persist operational state needed for delivery coordination, job tracking, and multi-instance processing patterns. This page summarizes the persistence requirements behind that choice.

## Persistence Requirements

The dataclasses used for OPC UA Job Management need to be persisted in a form that supports schema evolution and can store nested structures without excessive mapping overhead. A document-oriented persistence approach fits those requirements well.

The framework also needs persistence support for CloudEvent deduplication and for coordinating state publication so that changed variable values are emitted by a single publisher in multi-instance scenarios.

## [Redis](https://redis.io/docs/latest/)

MicroDCS stores dataclasses in Redis JSON. The associated `dataschema` value is written to an additional `_dataschema` field and is derived from the model `Config` class during serialization.

For the OPC UA Job Management implementation, Redis eventing is used to coordinate a single-instance publisher from a multi-instance receiver and event-publisher setup. Events are published directly to MQTT, while state variables are published through the OPC UA pub-sub mechanism so unchanged values do not need to be sent repeatedly.

* [Redis course notes](https://medium.com/@krushnakr9/l3-l5-redis-course-3ebc9843925c)
* [Redis Labs Python developer path](https://github.com/redislabs-training/ru-dev-path-py)
* [RedisVL hash vs JSON guide](https://redis.io/docs/latest/develop/ai/redisvl/user_guide/hash_vs_json/) - JSON fits the nested model structure used by MicroDCS
* [Redis OM for Python: globally unique primary keys](https://redis.io/blog/introducing-redis-om-for-python/#Globally_unique_primary_keys) - not required here because MicroDCS does not generate new primary keys through Redis OM