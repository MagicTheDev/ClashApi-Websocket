
import asyncio
import websockets
import aiohttp
from pymitter import EventEmitter
import orjson

ee = EventEmitter()

async def hello():
    url = "http://127.0.0.1:8000/login"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"username" : "admin", "password" : "test"}) as response:
            token = await response.json()
            token = token["access_token"]

    async with websockets.connect(f"ws://localhost:8000/players?token={token}") as websocket:
        async for message in websocket:
            try:
                json_message = orjson.loads(message)
                field = json_message["type"]
                awaitable = ee.emit_async(field, message)
                await awaitable
            except:
                print(message)

async def hello2():
    url = "http://127.0.0.1:8000/login"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"username" : "test", "password" : "test"}) as response:
            token = await response.json()
            token = token["access_token"]

    async with websockets.connect(f"ws://localhost:8000/clans?token={token}") as websocket:
        async for message in websocket:
            print(message)


@ee.on("trophies")
def handle_event1_v1(event):
    print(event)


loop = asyncio.get_event_loop()
loop.create_task(hello())
#loop.create_task(hello2())
loop.run_forever()

