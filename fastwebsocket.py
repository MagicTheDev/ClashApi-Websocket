from typing import List
from uvicorn import Config, Server
from fastapi import FastAPI, WebSocket, Depends, Request, HTTPException, Query, WebSocketDisconnect, Response
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
from fastapi_jwt_auth.exceptions import AuthJWTException
from pydantic import BaseModel
from datetime import timedelta
from pymongo import InsertOne, UpdateOne
from coc import utils
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from base64 import b64decode as base64_b64decode
from json import loads as json_loads
from datetime import datetime
from dotenv import load_dotenv

import fastapi
import motor.motor_asyncio
import sys
import collections
import time
import aiohttp
import asyncio
import orjson
import pytz
import os

utc = pytz.utc
load_dotenv()

EMAIL_PW = os.getenv("EMAIL_PW")

#DATABASE
client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
new_looper = client.new_looper
user_db = new_looper.user_db
player_stats = new_looper.player_stats


CLAN_CLIENTS = set()
PLAYER_CLIENTS = set()
CLAN_CACHE = {}
PLAYER_CACHE = {}

LAYERED_FIELDS = ["achievements", "troops", "heroes", "spells"]

emails = []
passwords = []
for x in range(1,13):
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

@app.post('/playertracklist')
@limiter.limit("3/minute")
async def player_track(request: Request, update: UpdateList, Authorize: AuthJWT = Depends()):
    try:
        Authorize.jwt_required("websocket", token=update.token)
        decoded_token = Authorize.get_raw_jwt(update.token)
        user = decoded_token["sub"]
        result = await user_db.find_one({"username": user})
        elevated = result.get("elevated")
        if elevated is False:
            raise HTTPException(status_code=401, detail="Requires Elevated Permissions")
        tracked_list = result.get("tracked_players")
        if tracked_list is None:
            tracked_list = []
        tracked_list = [ele for ele in tracked_list if ele not in update.remove]
        tracked_list = list(set(tracked_list + update.add))
        await user_db.update_one({"username": user}, {"$set" : {"tracked_players" : tracked_list}})
        return {"Updated" : tracked_list}
    except AuthJWTException as err:
        return("Invalid Token")

@app.post('/clantracklist')
@limiter.limit("10/second")
async def clan_track(request: Request, update: UpdateList, Authorize: AuthJWT = Depends()):
    try:
        Authorize.jwt_required("websocket", token=update.token)
        decoded_token = Authorize.get_raw_jwt(update.token)
        user = decoded_token["sub"]
        result = await user_db.find_one({"username": user})
        tracked_list = result.get("tracked_clans")
        if tracked_list is None:
            tracked_list = []
        tracked_list = [ele for ele in tracked_list if ele not in update.remove]
        tracked_list = list(set(tracked_list + update.add))
        await user_db.update_one({"username": user}, {"$set" : {"tracked_clans" : tracked_list}})
        return {"Updated" : tracked_list}
    except AuthJWTException as err:
        return("Invalid Token")

@app.get("/armyids")
@limiter.limit("10/second")
async def test(request : Request, response: Response):
    return {"troops" : {
        0: "Barbarian",
        1: "Archer",
        2: "Goblin",
        3: "Giant",
        4: "Wall Breaker",
        5: "Balloon",
        6: "Wizard",
        7: "Healer",
        8: "Dragon",
        9: "P.E.K.K.A",
        10: "Minion",
        11: "Hog Rider",
        12: "Valkyrie",
        13: "Golem",
        15: "Witch",
        17: "Lava Hound",
        22: "Bowler",
        23: "Baby Dragon",
        24: "Miner",
        26: "Super Barbarian",
        27: "Super Archer",
        28: "Super Wall Breaker",
        29: "Super Giant",
        53: "Yeti",
        55: "Sneaky Goblin",
        57: "Rocket Balloon",
        58: "Ice Golem",
        59: "Electro Dragon",
        63: "Inferno Dragon",
        64: "Super Valkyrie",
        65: "Dragon Rider",
        66: "Super Witch",
        76: "Ice Hound",
        80 : "Super Bowler",
        81 : "Super Dragon",
        82: "Headhunter",
        83: "Super Wizard",
        84: "Super Minion",
    },
            "sieges" : {51: "Wall Wrecker",
        52: "Battle Blimp",
        62: "Stone Slammer",
        75: "Siege Barracks",
        87: "Log Launcher",
        91 : "Flame Flinger"},
            "spells" : {
        0: "Lightning Spell",
        1: "Healing Spell",
        2: "Rage Spell",
        3: "Jump Spell",
        5: "Freeze Spell",
        9: "Poison Spell",
        10: "Earthquake Spell",
        11: "Haste Spell",
        16: "Clone Spell",
        17: "Skeleton Spell",
        28: "Bat Spell",
        35: "Invisibility Spell"
    }
            }

@app.get("/legends/{player_tag}")
@limiter.limit("30/second")
async def legends(request : Request, response: Response):
 return "WIP"

@app.get("/donations/{player_tag}")
@limiter.limit("30/second")
async def donations(request : Request, response: Response):
 return "WIP"

@app.get("/raids/{player_tag}")
@limiter.limit("30/second")
async def raids(request : Request, response: Response):
 return "WIP"

@app.get("/clangames/{player_tag}")
@limiter.limit("30/second")
async def clangames(request : Request, response: Response):
 return "WIP"

#leaderboard endpoint to get player

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

async def broadcast():
    try:
        while True:
            clan_tags = await user_db.distinct("tracked_clans")
            rtime = time.time()
            global keys

            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.read()
                    return player_

            tasks = []
            connector = aiohttp.TCPConnector(limit=1000)
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

            print(f"\nClan Fetch Time: {time.time() - rtime}")
            rtime = time.time()

            CLAN_MEMBERS = []
            for response in responses:
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
                if str(previous_response) != str(response):
                    CLAN_CACHE[tag] = response
                    for ws in CLAN_CLIENTS:
                        ws : fastapi.WebSocket
                        json_data = {"Clan Event" : {"old_clan": previous_response, "new_clan" : response}}
                        if previous_response is not None:
                            await ws.send_json(json_data)

            print(f"Send events for clan change: {time.time() - rtime}")
            print("The size of the clan dictionary is {} bytes".format(sys.getsizeof(CLAN_CACHE)))
            rtime = time.time()

            legend_tags = await user_db.distinct("tracked_players")
            db_tags = await player_stats.distinct("tag")
            all_tags_to_track = list(set(legend_tags + CLAN_MEMBERS))
            missing_tags = list(set(all_tags_to_track) - set(db_tags))

            #GENERATE DATES FOR STAT COLLECTION
            raid_date = gen_raid_date()
            season = gen_season_date()
            legend_date = gen_legend_date()

            #bulk add missing entries
            changes = []
            for tag in missing_tags:
                changes.append(InsertOne({"tag" : tag}))

            if changes != []:
                results = await player_stats.bulk_write(changes)
                print(f"Missing Player Update db time: {time.time() - rtime}")
            rtime = time.time()

            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.read()
                    return player_

            tasks = []
            connector = aiohttp.TCPConnector(limit=3000)
            async with aiohttp.ClientSession(connector=connector) as session2:
                for tag in all_tags_to_track:
                    headers = {"Authorization": f"Bearer {keys[0]}"}
                    tag = tag.replace("#", "%23")
                    url = f"https://api.clashofclans.com/v1/players/{tag}"
                    keys = collections.deque(keys)
                    keys.rotate(1)
                    keys = list(keys)
                    task = asyncio.ensure_future(fetch(url, session2, headers))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                await session2.close()

            print(f"Player Fetch Time: {time.time() - rtime}")
            rtime = time.time()

            changes = []
            for response in responses:
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
                    for achievement in response["achievements"][:]:
                        if achievement["name"] not in KEPT_ACHIEVEMENTS:
                            response["achievements"].remove(achievement)
                except:
                    pass

                if str(previous_response) != str(response):
                    '''
                    from jsondiff import diff
                    if diff(response, previous_response) is not None:
                        print(diff(response, previous_response))
                    '''

                    PLAYER_CACHE[tag] = response
                    for ws in PLAYER_CLIENTS:
                        ws : fastapi.WebSocket
                        json_data = {"Player Event" : {"old_player": previous_response, "new_player" : response}}
                        if previous_response is not None:
                            await ws.send_json(json_data)
                    if previous_response is not None:
                        clan_tag = "Unknown"
                        league = "None"
                        try:
                            clan_tag = response["clan"]["tag"]
                        except:
                            pass
                        try:
                            league = response["league"]["name"]
                        except:
                            pass

                        #OUTGOING DONO CHANGE
                        if response["donations"] != previous_response["donations"]:
                            previous_dono = previous_response["donations"]
                            if previous_dono > response["donations"]:
                                previous_dono = 0
                            diff = response["donations"] - previous_dono
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"donations.{season}.donated": diff}}))
                        #RECEIVED DONO CHANGE
                        if response["donationsReceived"] != previous_response["donationsReceived"]:
                            previous_dono = previous_response["donationsReceived"]
                            if previous_dono > response["donationsReceived"]:
                                previous_dono = 0
                            diff = response["donationsReceived"] - previous_dono
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"donations.{season}.received": diff}}))
                        #CAPITAL GOLD DONO CHANGE
                        if response["achievements"][-1]["value"] != previous_response["achievements"][-1]["value"]:
                            diff = response["achievements"][-1]["value"] - previous_response["achievements"][-1]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"capital_gold.{raid_date}.donated": diff}}))
                        # CAPITAL GOLD RAID CHANGE
                        if response["achievements"][-2]["value"] != previous_response["achievements"][-2]["value"]:
                            diff = response["achievements"][-2]["value"] - previous_response["achievements"][-2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"capital_gold.{raid_date}.raided": diff}}))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"capital_gold.{raid_date}.raided_clan": clan_tag}}))

                        #CLAN GAME CHANGE
                        if response["achievements"][2]["value"] != previous_response["achievements"][2]["value"]:
                            diff = response["achievements"][2]["value"] - previous_response["achievements"][2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"clan_games.{season}.points": diff}}))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"clan_games.{season}.clan": clan_tag}}))

                        #LEGENDS CHANGES
                        if response["trophies"] >= 4900 and league == "Legend League":
                            if response["trophies"] != previous_response["trophies"]:
                                diff_trophies = response["trophies"] - previous_response["trophies"]
                                diff_attacks = response["attackWins"] - previous_response["attackWins"]
                                if diff_trophies <= - 1:
                                    diff_trophies = abs(diff_trophies)
                                    if diff_trophies <= 100:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": diff_trophies}}))
                                elif diff_trophies >= 1:
                                    changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.{legend_date}.num_attacks": diff_attacks}}))
                                    if diff_attacks == 1:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": diff_trophies}}))
                                        if diff_trophies == 40:
                                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": 1}}))
                                        else:
                                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}))
                                    elif int(diff_trophies / 40) == diff_attacks:
                                        for x in range(0, diff_attacks):
                                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": 40}}))
                                        changes.append(UpdateOne({"tag": tag}, {"$inc": {f"legends.streak": diff_attacks}}))
                                    else:
                                        changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.attacks": diff_trophies}}))
                                        changes.append(UpdateOne({"tag": tag}, {"$set": {f"legends.streak": 0}}))

                            if response["defenseWins"] != previous_response["defenseWins"]:
                                diff_defenses = response["defenseWins"] - previous_response["defenseWins"]
                                for x in range(0, diff_defenses):
                                    changes.append(UpdateOne({"tag": tag}, {"$push": {f"legends.{legend_date}.defenses": 0}}))

            print(f"Send events & record player changes: {time.time() - rtime}")
            print("The size of the player dictionary is {} bytes".format(sys.getsizeof(PLAYER_CACHE)))

            rtime = time.time()
            if changes != []:
                results = await player_stats.bulk_write(changes)
                #print(results.bulk_api_result)
                print(f"Update db time: {time.time() - rtime}")
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
config = Config(app=app, loop="asyncio", port=8000)
server = Server(config)
loop.create_task(server.serve())
loop.create_task(broadcast())
loop.run_forever()


