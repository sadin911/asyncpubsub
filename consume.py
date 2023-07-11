from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio

from quart import Quart

app = Quart(__name__)

kafka_bootstrap_servers = 'localhost:29092'
consumer_topic = 'publishing-topic'
notification_topic = 'notification-topic'


async def consume_messages():
    consumer = AIOKafkaConsumer(
        consumer_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id='server2-group'
    )
    await consumer.start()

    try:
        async for message in consumer:
            received_message = message.value.decode()
            print(f"Received message: {received_message}")

            # Perform any processing required with the received message

            producer = AIOKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers
            )
            await producer.start()
            await producer.send(notification_topic, value=b'Processing complete')
            await producer.stop()
    finally:
        await consumer.stop()


@app.route('/consumeMessages', methods=['GET'])
async def consume_messages_handler():
    asyncio.ensure_future(consume_messages())
    return 'Started consuming messages'


if __name__ == '__main__':
    app.run(port=4000)
