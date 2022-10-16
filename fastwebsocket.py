from typing import List

from uvicorn import Config, Server
from fastapi import FastAPI, WebSocket, Depends, Request, HTTPException, Query, WebSocketDisconnect, Response
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
from fastapi_jwt_auth.exceptions import AuthJWTException
from pydantic import BaseModel
from pymongo import UpdateOne
from coc import utils
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from base64 import b64decode as base64_b64decode
from json import loads as json_loads
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import fastapi
import motor.motor_asyncio
import collections
import time
import aiohttp
import asyncio
import orjson
import pytz
import os
import coc

utc = pytz.utc
load_dotenv()
scheduler = AsyncIOScheduler(timezone=utc)
scheduler.start()

EMAIL_PW = os.getenv("EMAIL_PW")
DB_LOGIN = os.getenv("DB_LOGIN")

#DATABASE
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db_client = motor.motor_asyncio.AsyncIOMotorClient(DB_LOGIN)
new_looper = client.new_looper
user_db = new_looper.user_db
player_stats = new_looper.player_stats
leaderboard_db = new_looper.leaderboard_db
war_stats = new_looper.war_stats
persist = new_looper.persist

usafam = db_client.usafam
clan_db = usafam.clans

CLAN_CLIENTS = set()
PLAYER_CLIENTS = set()
WAR_CLIENTS = set()

CLAN_CACHE = {}
PLAYER_CACHE = {}
WAR_CACHE = {}

LAYERED_FIELDS = ["achievements", "troops", "heroes", "spells"]
LOCATIONS = ["global", 32000007, 32000008, 32000009, 32000010, 32000011, 32000012, 32000013, 32000014, 32000015, 32000016, 32000017,
             32000018, 32000019, 32000020, 32000021, 32000022, 32000023, 32000024, 32000025, 32000026, 32000027, 32000028,
             32000029, 32000030, 32000031, 32000032, 32000033, 32000034, 32000035, 32000036, 32000037, 32000038, 32000039,
             32000040, 32000041, 32000042, 32000043, 32000044, 32000045, 32000046, 32000047, 32000048, 32000049, 32000050,
             32000051, 32000052, 32000053, 32000054, 32000055, 32000056, 32000057, 32000058, 32000059, 32000060, 32000061,
             32000062, 32000063, 32000064, 32000065, 32000066, 32000067, 32000068, 32000069, 32000070, 32000071, 32000072,
             32000073, 32000074, 32000075, 32000076, 32000077, 32000078, 32000079, 32000080, 32000081, 32000082, 32000083,
             32000084, 32000085, 32000086, 32000087, 32000088, 32000089, 32000090, 32000091, 32000092, 32000093, 32000094,
             32000095, 32000096, 32000097, 32000098, 32000099, 32000100, 32000101, 32000102, 32000103, 32000104, 32000105,
             32000106, 32000107, 32000108, 32000109, 32000110, 32000111, 32000112, 32000113, 32000114, 32000115, 32000116,
             32000117, 32000118, 32000119, 32000120, 32000121, 32000122, 32000123, 32000124, 32000125, 32000126, 32000127,
             32000128, 32000129, 32000130, 32000131, 32000132, 32000133, 32000134, 32000135, 32000136, 32000137, 32000138,
             32000139, 32000140, 32000141, 32000142, 32000143, 32000144, 32000145, 32000146, 32000147, 32000148, 32000149,
             32000150, 32000151, 32000152, 32000153, 32000154, 32000155, 32000156, 32000157, 32000158, 32000159, 32000160,
             32000161, 32000162, 32000163, 32000164, 32000165, 32000166, 32000167, 32000168, 32000169, 32000170, 32000171,
             32000172, 32000173, 32000174, 32000175, 32000176, 32000177, 32000178, 32000179, 32000180, 32000181, 32000182,
             32000183, 32000184, 32000185, 32000186, 32000187, 32000188, 32000189, 32000190, 32000191, 32000192, 32000193,
             32000194, 32000195, 32000196, 32000197, 32000198, 32000199, 32000200, 32000201, 32000202, 32000203, 32000204,
             32000205, 32000206, 32000207, 32000208, 32000209, 32000210, 32000211, 32000212, 32000213, 32000214, 32000215,
             32000216, 32000217, 32000218, 32000219, 32000220, 32000221, 32000222, 32000223, 32000224, 32000225, 32000226,
             32000227, 32000228, 32000229, 32000230, 32000231, 32000232, 32000233, 32000234, 32000235, 32000236, 32000237,
             32000238, 32000239, 32000240, 32000241, 32000242, 32000243, 32000244, 32000245, 32000246, 32000247, 32000248,
             32000249, 32000250, 32000251, 32000252, 32000253, 32000254, 32000255, 32000256, 32000257, 32000258, 32000259, 32000260]

emails = []
passwords = []
for x in range(1,12):
    emails.append(f"apiclashofclans+test{x}@gmail.com")
    passwords.append(EMAIL_PW)


async def get_keys(emails: list, passwords: list, key_names: str, key_count: int):
    total_keys = []

    for count, email in enumerate(emails):
        _keys = []
        password = passwords[count]

        session = aiohttp.ClientSession()

        body = {"email": email, "password": password}
        resp = await session.post("https://developer.clashofclans.com/api/login", json=body)
        if resp.status == 403:
            raise RuntimeError(
                "Invalid Credentials"
            )

        resp_paylaod = await resp.json()
        ip = json_loads(base64_b64decode(resp_paylaod["temporaryAPIToken"].split(".")[1] + "====").decode("utf-8"))[
            "limits"][1]["cidrs"][0].split("/")[0]

        resp = await session.post("https://developer.clashofclans.com/api/apikey/list")
        keys = (await resp.json())["keys"]
        _keys.extend(key["key"] for key in keys if key["name"] == key_names and ip in key["cidrRanges"])

        if len(_keys) < key_count:
            for key in (k for k in keys if k["name"] == key_names and ip not in k["cidrRanges"]):
                await session.post("https://developer.clashofclans.com/api/apikey/revoke", json={"id": key["id"]})

            while len(_keys) < key_count and len(keys) < 10:
                data = {
                    "name": key_names,
                    "description": "Created on {}".format(datetime.now().strftime("%c")),
                    "cidrRanges": [ip],
                    "scopes": ["clash"],
                }

                resp = await session.post("https://developer.clashofclans.com/api/apikey/create", json=data)
                key = await resp.json()
                _keys.append(key["key"]["key"])

        if len(keys) == 10 and len(_keys) < key_count:
            print("%s keys were requested to be used, but a maximum of %s could be "
                  "found/made on the developer site, as it has a maximum of 10 keys per account. "
                  "Please delete some keys or lower your `key_count` level."
                  "I will use %s keys for the life of this client.", )

        if len(_keys) == 0:
            raise RuntimeError(
                "There are {} API keys already created and none match a key_name of '{}'."
                "Please specify a key_name kwarg, or go to 'https://developer.clashofclans.com' to delete "
                "unused keys.".format(len(keys), key_names)
            )

        await session.close()
        #print("Successfully initialised keys for use.")
        for k in _keys:
            total_keys.append(k)

    print(len(total_keys))
    return (total_keys)

def create_keys():
    done = False
    while done is False:
        try:
            loop = asyncio.get_event_loop()
            keys = loop.run_until_complete(get_keys(emails=emails,
                                     passwords=passwords, key_names="test", key_count=10))
            done = True
            return keys
        except Exception as e:
            done = False
            print(e)

keys = create_keys()

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

class User(BaseModel):
    username: str
    password: str

class Settings(BaseModel):
    authjwt_secret_key: str = "secret"

class UpdateList(BaseModel):
    add : List[str]
    remove : List[str]
    token : str

@AuthJWT.load_config
def get_config():
    return Settings()

@app.exception_handler(AuthJWTException)
def authjwt_exception_handler(request: Request, exc: AuthJWTException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.message}
    )

@app.post('/login')
async def login(user: User, Authorize: AuthJWT = Depends()):
    result = await user_db.find_one({"username": user.username})
    if result is None:
        raise HTTPException(status_code=401,detail="Bad username or password")
    else:
        stored_pw = result.get("password")
        if stored_pw != user.password:
            raise HTTPException(status_code=401, detail="Bad username or password")
    access_token = Authorize.create_access_token(subject=user.username,fresh=True)
    return {"access_token": access_token}

@app.get('/servers')
async def server_():
    servers = await db_client.usafam.server.distinct("server")
    return {"servers": servers}

@app.websocket("/players")
async def player_websocket(websocket: WebSocket, token: str = Query(...), Authorize: AuthJWT = Depends()):
    await websocket.accept()
    PLAYER_CLIENTS.add(websocket)
    try:
        Authorize.jwt_required("websocket", token=token)
        await websocket.send_text("Successfully Login!")
        decoded_token = Authorize.get_raw_jwt(token)
        await websocket.send_text(f"Here your decoded token: {decoded_token}")
        try:
            while True:
                data = await websocket.receive_text()
        except WebSocketDisconnect:
            PLAYER_CLIENTS.remove(websocket)

    except AuthJWTException as err:
        await websocket.send_text(err.message)
        await websocket.close()

@app.websocket("/clans")
async def clan_websocket(websocket: WebSocket, token: str = Query(...), Authorize: AuthJWT = Depends()):
    await websocket.accept()
    CLAN_CLIENTS.add(websocket)
    try:
        Authorize.jwt_required("websocket", token=token)
        await websocket.send_text("Successfully Login!")
        decoded_token = Authorize.get_raw_jwt(token)
        await websocket.send_text(f"Here your decoded token: {decoded_token}")
        try:
            while True:
                data = await websocket.receive_text()
        except WebSocketDisconnect:
            CLAN_CLIENTS.remove(websocket)
    except AuthJWTException as err:
        await websocket.send_text(err.message)
        await websocket.close()

@app.websocket("/wars")
async def war_websocket(websocket: WebSocket, token: str = Query(...), Authorize: AuthJWT = Depends()):
    await websocket.accept()
    WAR_CLIENTS.add(websocket)
    try:
        Authorize.jwt_required("websocket", token=token)
        await websocket.send_text("Successfully Login!")
        decoded_token = Authorize.get_raw_jwt(token)
        await websocket.send_text(f"Here your decoded token: {decoded_token}")
        try:
            while True:
                data = await websocket.receive_text()
        except WebSocketDisconnect:
            WAR_CLIENTS.remove(websocket)
    except AuthJWTException as err:
        await websocket.send_text(err.message)
        await websocket.close()

async def broadcast():

    start = True
    while True:
        try:
            rtime = time.time()
            clan_tags = await clan_db.distinct("tag")
            global keys
            global PLAYER_CACHE
            global CLAN_CACHE

            #CLAN EVENTS
            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.read()
                    return player_

            tasks = []
            connector = aiohttp.TCPConnector(limit=100)
            async with aiohttp.ClientSession(connector=connector) as session:
                for tag in clan_tags:
                    headers = {"Authorization": f"Bearer {keys[0]}"}
                    tag = tag.replace("#", "%23")
                    url = f"https://api.clashofclans.com/v1/clans/{tag}"
                    keys = collections.deque(keys)
                    keys.rotate(1)
                    keys = list(keys)
                    task = asyncio.ensure_future(fetch(url, session, headers))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                await session.close()

            print(f"CLAN FETCH: {time.time()-rtime}")
            CLAN_MEMBERS = []
            clan_tasks = []
            cc_copy = CLAN_CLIENTS.copy()
            for response in responses:
                async def send_ws(ws, json):
                    try:
                        await ws.send_json(json)
                    except:
                        try:
                            CLAN_CLIENTS.remove(ws)
                        except:
                            pass

                try:
                    response = orjson.loads(response)
                    tag = response["tag"]
                except:
                    continue

                del response["clanPoints"]
                del response["clanVersusPoints"]
                for x in range(0, len(response["memberList"])):
                    del response["memberList"][x]["expLevel"]
                    try:
                        del response["memberList"][x]["league"]
                    except:
                        pass
                    del response["memberList"][x]["trophies"]
                    del response["memberList"][x]["versusTrophies"]
                    del response["memberList"][x]["clanRank"]
                    del response["memberList"][x]["previousClanRank"]
                    del response["memberList"][x]["donations"]
                    del response["memberList"][x]["donationsReceived"]

                CLAN_MEMBERS += [member["tag"] for member in response["memberList"]]
                try:
                    previous_response = CLAN_CACHE[tag]
                except:
                    previous_response = None

                CLAN_CACHE[tag] = response
                from jsondiff import diff
                if previous_response is not None:
                    if str(previous_response) != str(response):
                        for ws in cc_copy:
                            ws: fastapi.WebSocket
                            if diff(response, previous_response) is not None:
                                differences = diff(response, previous_response)
                                for dif in differences:
                                    if "$delete" not in str(dif):
                                        json_data = {"type": dif, "old_clan": previous_response,
                                                     "new_clan": response}
                                        task = asyncio.ensure_future(send_ws(ws=ws, json=json_data))
                                        clan_tasks.append(task)

            await asyncio.gather(*clan_tasks, return_exceptions=False)
            print(f"CLAN RESPONSES: {time.time() - rtime}")

            print(len(CLAN_MEMBERS))
            db_tags = await player_stats.distinct("tag")
            all_tags_to_track = list(set(db_tags + CLAN_MEMBERS))
            print(f"{len(all_tags_to_track)} tags")
            #all_tags_to_track = all_tags_to_track[0:100]

            #GENERATE DATES FOR STAT COLLECTION
            raid_date = gen_raid_date()
            season = gen_season_date()
            legend_date = gen_legend_date()


            #PLAYER EVENTS
            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.read()
                    return player_

            tasks = []
            connector = aiohttp.TCPConnector(limit_per_host=3000)
            async with aiohttp.ClientSession(connector=connector) as session3:
                for tag in all_tags_to_track:
                    headers = {"Authorization": f"Bearer {keys[0]}"}
                    tag = tag.replace("#", "%23")
                    url = f"https://api.clashofclans.com/v1/players/{tag}"
                    keys = collections.deque(keys)
                    keys.rotate(1)
                    keys = list(keys)
                    task = asyncio.ensure_future(fetch(url, session3, headers))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                await session3.close()
            print(f"FETCH PLAYERS: {time.time() - rtime}")

            changes = []
            lt = 0
            zz = 0
            ws_tasks = []
            for response in responses:
                response_start = time.time()
                BEEN_ONLINE = False
                KEPT_ACHIEVEMENTS = ["Gold Grab", "Elixir Escapade", "Games Champion", "Aggressive Capitalism", "Most Valuable Clanmate"]
                try:
                    response = orjson.loads(response)
                    tag = response["tag"]
                except:
                    continue
                try:
                    previous_response = PLAYER_CACHE[tag]
                except:
                    previous_response = None

                try:
                    del response["legendStatistics"]
                except:
                    pass

                for achievement in response["achievements"][:]:
                    if achievement["name"] not in KEPT_ACHIEVEMENTS:
                        try:
                            response["achievements"].remove(achievement)
                        except:
                            pass

                PLAYER_CACHE[tag] = response
                if previous_response is not None:
                    if str(previous_response) != str(response):
                        #from jsondiff import diff
                        pc_copy =PLAYER_CLIENTS.copy()
                        if zz < 5:
                            print(f"TIME TO CONVERT JSON & DEL STUFF: {time.time()- response_start}")
                            zz += 1

                        clan_tag = "Unknown"
                        try:
                            clan_tag = response["clan"]["tag"]
                        except:
                            pass
                        try:
                            league = response["league"]["name"]
                        except:
                            league = "Unranked"

                        try:
                            prev_league = previous_response["league"]["name"]
                        except:
                            prev_league = "Unranked"

                        if league != prev_league:
                            changes.append(UpdateOne({"tag": tag}, {"$set": {"league": league}}))

                        async def send_ws(ws, json):
                            try:
                                await ws.send_json(json)
                            except:
                                try:
                                    PLAYER_CLIENTS.remove(ws)
                                except:
                                    pass

                        #OUTGOING DONO CHANGE
                        if response["donations"] != previous_response["donations"]:
                            BEEN_ONLINE = True
                            previous_dono = previous_response["donations"]
                            if previous_dono > response["donations"]:
                                previous_dono = 0
                            diff = response["donations"] - previous_dono
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"donations.{season}.donated": diff}}, upsert=True))

                        #RECEIVED DONO CHANGE
                        if response["donationsReceived"] != previous_response["donationsReceived"]:
                            previous_dono = previous_response["donationsReceived"]
                            if previous_dono > response["donationsReceived"]:
                                previous_dono = 0
                            diff = response["donationsReceived"] - previous_dono
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"donations.{season}.received": diff}}, upsert=True))

                        #CAPITAL GOLD DONO CHANGE
                        if response["achievements"][-1]["value"] != previous_response["achievements"][-1]["value"]:
                            BEEN_ONLINE = True
                            diff = response["achievements"][-1]["value"] - previous_response["achievements"][-1]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"capital_gold.{raid_date}.donate": diff}}, upsert=True))
                            for ws in pc_copy:
                                ws: fastapi.WebSocket
                                # await
                                json_data = {"type": "Most Valuable Clanmate", "old_player": previous_response,"new_player": response}
                                task = asyncio.ensure_future(send_ws(ws=ws, json=json_data))
                                ws_tasks.append(task)

                        # CAPITAL GOLD RAID CHANGE
                        if response["achievements"][-2]["value"] != previous_response["achievements"][-2]["value"]:
                            BEEN_ONLINE = True
                            diff = response["achievements"][-2]["value"] - previous_response["achievements"][-2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"capital_gold.{raid_date}.raid": diff}}, upsert=True))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"capital_gold.{raid_date}.raided_clan": clan_tag}}, upsert=True))
                            for ws in pc_copy:
                                ws: fastapi.WebSocket
                                # await
                                json_data = {"type": "Aggressive Capitalism", "old_player": previous_response,"new_player": response}
                                task = asyncio.ensure_future(send_ws(ws=ws, json=json_data))
                                ws_tasks.append(task)

                        #CLAN GAME CHANGE
                        if response["achievements"][2]["value"] != previous_response["achievements"][2]["value"]:
                            BEEN_ONLINE = True
                            diff = response["achievements"][2]["value"] - previous_response["achievements"][2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"clan_games.{season}.points": diff}}, upsert=True))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"clan_games.{season}.clan": clan_tag}}, upsert=True))

                        # NAME CHANGE
                        if response["name"] != previous_response["name"]:
                            BEEN_ONLINE = True
                            changes.append(UpdateOne({"tag": tag}, {"$set": {"name": response["name"]}}))

                        if response["trophies"] != previous_response["trophies"]:
                            BEEN_ONLINE = True

                        #LEGENDS CHANGES
                        if response["trophies"] >= 4900 and league == "Legend League":
                            if response["trophies"] != previous_response["trophies"]:
                                if lt < 5:
                                    print(f"tag {tag}, {response['trophies']}, {previous_response['trophies']}")
                                    lt+=1
                                for ws in pc_copy:
                                    ws: fastapi.WebSocket
                                    # await
                                    json_data = {"type": "trophies", "old_player": previous_response, "new_player": response}
                                    task = asyncio.ensure_future(send_ws(ws=ws, json=json_data))
                                    ws_tasks.append(task)

                                diff_trophies = response["trophies"] - previous_response["trophies"]
                                diff_attacks = response["attackWins"] - previous_response["attackWins"]
                                if diff_trophies <= - 1:
                                    diff_trophies = abs(diff_trophies)
                                    if diff_trophies <= 100:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": diff_trophies}}, upsert=True))
                                elif diff_trophies >= 1:
                                    changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.{legend_date}.num_attacks": diff_attacks}}, upsert=True))
                                    if diff_attacks == 1:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": diff_trophies}}, upsert=True))
                                        if diff_trophies == 40:
                                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": 1}}))
                                        else:
                                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}))
                                    elif int(diff_trophies / 40) == diff_attacks:
                                        for x in range(0, diff_attacks):
                                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": 40}}, upsert=True))
                                        changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": diff_attacks}}))
                                    else:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": diff_trophies}}, upsert=True))
                                        changes.append(UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}, upsert=True))

                            if response["defenseWins"] != previous_response["defenseWins"]:
                                diff_defenses = response["defenseWins"] - previous_response["defenseWins"]
                                for x in range(0, diff_defenses):
                                    changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": 0}}, upsert=True))


                        if BEEN_ONLINE:
                            _time = int(datetime.now().timestamp())
                            changes.append(UpdateOne({"tag": tag}, {"$set": {"last_online": _time}}))

            print(f"PLAYER RESPONSE DONE: {time.time() - rtime}")

            responses = await asyncio.gather(*ws_tasks, return_exceptions=False)

            print(f"SENT ALL WS: {time.time() - rtime}")


            if changes != []:
                results = await player_stats.bulk_write(changes)
                print(results.bulk_api_result)
                print(f"CHANGES INSERT: {time.time() - rtime}")


            name_changes = []
            if len(PLAYER_CACHE) != 0:
                no_named = player_stats.find({"name": None})
                limit = await player_stats.count_documents(filter={"name": None})
                for player in await no_named.to_list(length=limit):
                    tag = player.get("tag")
                    try:
                        name = PLAYER_CACHE[tag]["name"]
                        name_changes.append(UpdateOne({"tag": tag}, {"$set": {"name": name}}))
                    except:
                        continue

            print(f"FETCH NO NAME: {time.time() - rtime}")
            if name_changes != []:
                results = await player_stats.bulk_write(name_changes)
                print(results.bulk_api_result)
                print(f"NO NAME CHANGES: {time.time() - rtime}")

            print(f"DONE: {time.time() - rtime}")
        except Exception as e:
            print(e)


def gen_raid_date():
    now = datetime.utcnow().replace(tzinfo=utc)
    current_dayofweek = now.weekday()
    if (current_dayofweek == 4 and now.hour >= 7) or (current_dayofweek == 5) or (current_dayofweek == 6) or (
            current_dayofweek == 0 and now.hour < 7):
        if current_dayofweek == 0:
            current_dayofweek = 7
        fallback = current_dayofweek - 4
        raidDate = (now - timedelta(fallback)).date()
        return str(raidDate)
    else:
        forward = 4 - current_dayofweek
        raidDate = (now + timedelta(forward)).date()
        return str(raidDate)

def gen_season_date():
    start = utils.get_season_start().replace(tzinfo=utc).date()
    year_add = 0
    if start == 12:
        start = 0
        year_add += 1
    month = start.month + 1
    if month <= 9:
        month = f"0{month}"
    year = start.year + year_add
    return f"{year}-{month}"

def gen_legend_date():
    now = datetime.utcnow()
    hour = now.hour
    if hour < 5:
        date = (now - timedelta(1)).date()
    else:
        date = now.date()
    return str(date)


loop = asyncio.get_event_loop()
config = Config(app=app, loop="asyncio", host = "104.251.216.53", port=8000,ws_ping_interval=120 ,ws_ping_timeout= 120)
server = Server(config)
loop.create_task(server.serve())
loop.create_task(broadcast())
loop.run_forever()


