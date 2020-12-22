import kafka
import kafka.errors

from finitestate.common.retry_utils import retry


@retry(on=kafka.errors.NoBrokersAvailable)
def create_kafka_consumer(*topics, **configs) -> kafka.KafkaConsumer:
    return kafka.KafkaConsumer(*topics, **configs)


@retry(on=kafka.errors.NoBrokersAvailable)
def create_kafka_producer(**configs) -> kafka.KafkaProducer:
    return kafka.KafkaProducer(**configs)


@retry(on=kafka.errors.NoBrokersAvailable)
def create_kafka_admin_client(**configs) -> kafka.KafkaAdminClient:
    return kafka.KafkaAdminClient(**configs)
