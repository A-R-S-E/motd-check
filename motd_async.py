import aio_pika
import asyncio
from mcstatus import MinecraftServer
import json
import concurrent.futures
import socket
import os
from dotenv import load_dotenv
load_dotenv()


executor = None
pika_connection = None
pika_channel = None
pika_queue = None

workers_amount = 400

def check(ip, port):
    try:
        status = MinecraftServer.lookup("{}:{}".format(ip, port)).status(retries=1)
        return bool(status)
    except (socket.timeout, ConnectionRefusedError, ConnectionResetError, OSError):
        return False

async def consumer_func(message: aio_pika.IncomingMessage):
    json_body = json.loads(message.body.decode('utf-8'))
    task = loop.run_in_executor(executor, check, json_body['ip'], json_body['port'])
    res, _ = await asyncio.wait([task])
    if list(res)[0].result():
        await pika_channel.default_exchange.publish(message, routing_key=os.environ['RABBIT_MOTD_QUEUE'])
    message.ack()

async def connect_rabbitmq(loop):
    global pika_channel, pika_connection, pika_queue
    pika_connection = await aio_pika.connect_robust("amqp://{}:{}@{}:{}/{}".format(os.environ['RABBIT_USER'], os.environ['RABBIT_PW'], os.environ['RABBIT_HOST'], os.environ['RABBIT_PORT'], os.environ['RABBIT_VHOST']), loop=loop)
    pika_channel = await pika_connection.channel()
    await pika_channel.set_qos(prefetch_count=workers_amount)
    pika_queue = await pika_channel.declare_queue(os.environ['RABBIT_PORTS_QUEUE'], durable=True)
    await pika_queue.consume(consumer_func)
    return pika_connection, pika_channel, pika_queue

async def main(loop):
    await connect_rabbitmq(loop)

if __name__ == "__main__":
    executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=workers_amount,
        )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())