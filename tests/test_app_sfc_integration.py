import asyncio
import uuid

import pytest
import pytest_asyncio
import redis.asyncio as redis
from conftest import (
    app_available,
    build_example_work_master,
    integration,
    mqtt_available,
    redis_available,
)

from microdcs import MQTTConfig, RedisConfig, RuntimeConfig
from microdcs.common import CloudEvent
from microdcs.models.greetings import Hello
from microdcs.models.machinery_jobs import (
    ISA95EquipmentDataType,
    ISA95JobOrderDataType,
    ISA95WorkMasterDataType,
    LocalizedText,
    StoreAndStartCall,
)
from microdcs.mqtt import MQTTHandler
from microdcs.redis import (
    EquipmentListDAO,
    JobResponseDAO,
    RedisKeySchema,
    SfcExecutionDAO,
    WorkMasterDAO,
)

MQTT_CONFIG = MQTTConfig()
REDIS_CONFIG = RedisConfig()
REDIS_SCHEMA = RedisKeySchema(RuntimeConfig().redis.key_prefix)
GREETINGS_COMMAND_TOPIC = "app/greetings/+/commands"


@pytest_asyncio.fixture
async def mqtt_handler():
    redis_connection_pool = redis.ConnectionPool(
        host=REDIS_CONFIG.hostname, port=REDIS_CONFIG.port, protocol=3
    )
    mqtt_config = MQTTConfig()
    mqtt_config.identifier = f"test-sfc-app-{uuid.uuid4()}"
    handler = MQTTHandler(mqtt_config, redis_connection_pool, REDIS_SCHEMA)
    yield handler
    await redis_connection_pool.aclose()


async def _wait_for_completion(
    execution_dao: SfcExecutionDAO, job_id: str, timeout: float = 10.0
):
    async with asyncio.timeout(timeout):
        while True:
            exec_state = await execution_dao.retrieve(job_id)
            if exec_state is not None and exec_state.completed:
                return exec_state
            await asyncio.sleep(0.1)


@pytest.mark.asyncio
@integration
@mqtt_available
@redis_available
@app_available
async def test_example_app_completes_sfc_recipe_via_greetings_response(
    mqtt_handler: MQTTHandler,
):
    redis_client = redis.Redis(
        host=REDIS_CONFIG.hostname, port=REDIS_CONFIG.port, protocol=3
    )
    work_master_dao = WorkMasterDAO(redis_client, REDIS_SCHEMA)
    equipment_dao = EquipmentListDAO(redis_client, REDIS_SCHEMA)
    execution_dao = SfcExecutionDAO(redis_client, REDIS_SCHEMA)
    job_response_dao = JobResponseDAO(redis_client, REDIS_SCHEMA)

    run_id = uuid.uuid4().hex[:8]
    scope = f"sfc-demo-{run_id}"
    job_id = f"job-sfc-{run_id}"
    job_response_topic = f"tests/jobs/response/{run_id}"

    work_master = build_example_work_master()
    work_master.id = f"{work_master.id}-{run_id}"
    await work_master_dao.save(work_master, scope)
    await equipment_dao.add_to_list(scope, scope)

    job_order = ISA95JobOrderDataType(
        job_order_id=job_id,
        work_master_id=[ISA95WorkMasterDataType(id=work_master.id)],
        equipment_requirements=[ISA95EquipmentDataType(id=scope)],
    )
    store_and_start = StoreAndStartCall(
        job_order=job_order,
        comment=[LocalizedText(text="Start example SFC job", locale="en")],
    )
    store_ce = CloudEvent(
        source="https://example.com/tests/sfc-app",
        datacontenttype="application/json; charset=utf-8",
        subject=scope,
        transportmetadata={
            "mqtt_topic": f"app/jobs/{scope}/commands",
            "mqtt_response_topic": job_response_topic,
        },
    )
    store_ce.serialize_payload(store_and_start)

    mqtt_client = mqtt_handler._client()
    async with mqtt_client:
        await mqtt_client.subscribe(job_response_topic)
        await mqtt_client.subscribe(GREETINGS_COMMAND_TOPIC)
        await mqtt_handler._publish_message(mqtt_client, store_ce)

        job_response_received = False
        response_topic: str | None = None
        command_id: str | None = None

        async with asyncio.timeout(10.0):
            async for message in mqtt_client.messages:
                if str(message.topic) == job_response_topic:
                    job_response_received = True
                elif message.topic.matches(GREETINGS_COMMAND_TOPIC):
                    if message.properties and hasattr(
                        message.properties, "ResponseTopic"
                    ):
                        response_topic = str(message.properties.ResponseTopic)  # type: ignore[arg-type]
                    if message.properties and hasattr(
                        message.properties, "CorrelationData"
                    ):
                        command_id = str(
                            uuid.UUID(bytes=message.properties.CorrelationData)  # type: ignore[arg-type]
                        )
                if response_topic and command_id:
                    break

        assert job_response_received is True
        assert response_topic is not None
        assert command_id is not None

        response_ce = CloudEvent(
            source="https://example.com/tests/equipment",
            datacontenttype="application/json; charset=utf-8",
            causationid=command_id,
            transportmetadata={"mqtt_topic": response_topic},
        )
        response_ce.serialize_payload(Hello(name="Plant"))
        await mqtt_handler._publish_message(mqtt_client, response_ce)

    exec_state = await _wait_for_completion(execution_dao, job_id)
    assert exec_state.completed is True

    job_response = await job_response_dao.retrieve_by_job_order_id(job_id)
    assert job_response is not None
    assert job_response.job_order_id == job_id

    await redis_client.aclose()
