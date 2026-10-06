"""Optional live source: subscribe to Hornet MQTT and feed block ids to the pipeline.

Needs port 1883 of the hornet container published to the host (add "1883:1883" to
hornet-main.yaml) and MQTT_HOST set in .env. Topic `block-metadata/referenced` carries
JSON with the `blockId` of every block as it gets referenced by a milestone.
"""
import asyncio
import json
import logging

from .. import config

log = logging.getLogger("mqtt")


def start(loop: asyncio.AbstractEventLoop, on_block_id):
    if not config.MQTT_HOST:
        return None
    import paho.mqtt.client as mqtt

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:  # paho 1.x
        client = mqtt.Client()

    def on_connect(c, *_):
        c.subscribe("block-metadata/referenced")
        log.info("MQTT connected, subscribed to block-metadata/referenced")

    def on_message(_c, _u, msg):
        try:
            bid = json.loads(msg.payload).get("blockId")
        except ValueError:
            return
        if bid:
            asyncio.run_coroutine_threadsafe(on_block_id(bid), loop)

    client.on_connect, client.on_message = on_connect, on_message
    client.connect_async(config.MQTT_HOST, config.MQTT_PORT)
    client.loop_start()
    return client
