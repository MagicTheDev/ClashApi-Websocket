from typing import List

import coc.utils
from uvicorn import Config, Server
from fastapi import FastAPI, WebSocket, Depends, Request, HTTPException, Query, WebSocketDisconnect, Response
from fastapi.responses import JSONResponse
from fastapi_jwt_auth import AuthJWT
from fastapi_jwt_auth.exceptions import AuthJWTException
from pydantic import BaseModel
from pymongo import InsertOne, UpdateOne
from coc import utils
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from base64 import b64decode as base64_b64decode
from json import loads as json_loads
from datetime import datetime
from datetime import timedelta
from dotenv import load_dotenv

import fastapi
import motor.motor_asyncio
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
leaderboard_db = new_looper.leaderboard_db


CLAN_CLIENTS = set()
PLAYER_CLIENTS = set()
WAR_CLIENTS = set()
CLAN_CACHE = {}
PLAYER_CACHE = {}
LOCATION_CACHE = {}
GLOBAL_LOCATION_CACHE = {}
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
LOCATION_DATA = {32000007: ['AF', 'Afghanistan'], 32000008: ['AX', 'Åland Islands'], 32000009: ['AL', 'Albania'], 32000010: ['DZ', 'Algeria'], 32000011: ['AS', 'American Samoa'], 32000012: ['AD', 'Andorra'], 32000013: ['AO', 'Angola'], 32000014: ['AI', 'Anguilla'], 32000015: ['AQ', 'Antarctica'], 32000016: ['AG', 'Antigua and Barbuda'], 32000017: ['AR', 'Argentina'], 32000018: ['AM', 'Armenia'], 32000019: ['AW', 'Aruba'], 32000020: ['AC', 'Ascension Island'], 32000021: ['AU', 'Australia'], 32000022: ['AT', 'Austria'], 32000023: ['AZ', 'Azerbaijan'], 32000024: ['BS', 'Bahamas'], 32000025: ['BH', 'Bahrain'], 32000026: ['BD', 'Bangladesh'], 32000027: ['BB', 'Barbados'], 32000028: ['BY', 'Belarus'], 32000029: ['BE', 'Belgium'], 32000030: ['BZ', 'Belize'], 32000031: ['BJ', 'Benin'], 32000032: ['BM', 'Bermuda'], 32000033: ['BT', 'Bhutan'], 32000034: ['BO', 'Bolivia'], 32000035: ['BA', 'Bosnia and Herzegovina'], 32000036: ['BW', 'Botswana'], 32000037: ['BV', 'Bouvet Island'], 32000038: ['BR', 'Brazil'], 32000039: ['IO', 'British Indian Ocean Territory'], 32000040: ['VG', 'British Virgin Islands'], 32000041: ['BN', 'Brunei'], 32000042: ['BG', 'Bulgaria'], 32000043: ['BF', 'Burkina Faso'], 32000044: ['BI', 'Burundi'], 32000045: ['KH', 'Cambodia'], 32000046: ['CM', 'Cameroon'], 32000047: ['CA', 'Canada'], 32000048: ['IC', 'Canary Islands'], 32000049: ['CV', 'Cape Verde'], 32000050: ['BQ', 'Caribbean Netherlands'], 32000051: ['KY', 'Cayman Islands'], 32000052: ['CF', 'Central African Republic'], 32000053: ['EA', 'Ceuta and Melilla'], 32000054: ['TD', 'Chad'], 32000055: ['CL', 'Chile'], 32000056: ['CN', 'China'], 32000057: ['CX', 'Christmas Island'], 32000058: ['CC', 'Cocos (Keeling) Islands'], 32000059: ['CO', 'Colombia'], 32000060: ['KM', 'Comoros'], 32000061: ['CG', 'Congo (DRC)'], 32000062: ['CD', 'Congo (Republic)'], 32000063: ['CK', 'Cook Islands'], 32000064: ['CR', 'Costa Rica'], 32000065: ['CI', 'Côte d’Ivoire'], 32000066: ['HR', 'Croatia'], 32000067: ['CU', 'Cuba'], 32000068: ['CW', 'Curaçao'], 32000069: ['CY', 'Cyprus'], 32000070: ['CZ', 'Czech Republic'], 32000071: ['DK', 'Denmark'], 32000072: ['DG', 'Diego Garcia'], 32000073: ['DJ', 'Djibouti'], 32000074: ['DM', 'Dominica'], 32000075: ['DO', 'Dominican Republic'], 32000076: ['EC', 'Ecuador'], 32000077: ['EG', 'Egypt'], 32000078: ['SV', 'El Salvador'], 32000079: ['GQ', 'Equatorial Guinea'], 32000080: ['ER', 'Eritrea'], 32000081: ['EE', 'Estonia'], 32000082: ['ET', 'Ethiopia'], 32000083: ['FK', 'Falkland Islands'], 32000084: ['FO', 'Faroe Islands'], 32000085: ['FJ', 'Fiji'], 32000086: ['FI', 'Finland'], 32000087: ['FR', 'France'], 32000088: ['GF', 'French Guiana'], 32000089: ['PF', 'French Polynesia'], 32000090: ['TF', 'French Southern Territories'], 32000091: ['GA', 'Gabon'], 32000092: ['GM', 'Gambia'], 32000093: ['GE', 'Georgia'], 32000094: ['DE', 'Germany'], 32000095: ['GH', 'Ghana'], 32000096: ['GI', 'Gibraltar'], 32000097: ['GR', 'Greece'], 32000098: ['GL', 'Greenland'], 32000099: ['GD', 'Grenada'], 32000100: ['GP', 'Guadeloupe'], 32000101: ['GU', 'Guam'], 32000102: ['GT', 'Guatemala'], 32000103: ['GG', 'Guernsey'], 32000104: ['GN', 'Guinea'], 32000105: ['GW', 'Guinea-Bissau'], 32000106: ['GY', 'Guyana'], 32000107: ['HT', 'Haiti'], 32000108: ['HM', 'Heard & McDonald Islands'], 32000109: ['HN', 'Honduras'], 32000110: ['HK', 'Hong Kong'], 32000111: ['HU', 'Hungary'], 32000112: ['IS', 'Iceland'], 32000113: ['IN', 'India'], 32000114: ['ID', 'Indonesia'], 32000115: ['IR', 'Iran'], 32000116: ['IQ', 'Iraq'], 32000117: ['IE', 'Ireland'], 32000118: ['IM', 'Isle of Man'], 32000119: ['IL', 'Israel'], 32000120: ['IT', 'Italy'], 32000121: ['JM', 'Jamaica'], 32000122: ['JP', 'Japan'], 32000123: ['JE', 'Jersey'], 32000124: ['JO', 'Jordan'], 32000125: ['KZ', 'Kazakhstan'], 32000126: ['KE', 'Kenya'], 32000127: ['KI', 'Kiribati'], 32000128: ['XK', 'Kosovo'], 32000129: ['KW', 'Kuwait'], 32000130: ['KG', 'Kyrgyzstan'], 32000131: ['LA', 'Laos'], 32000132: ['LV', 'Latvia'], 32000133: ['LB', 'Lebanon'], 32000134: ['LS', 'Lesotho'], 32000135: ['LR', 'Liberia'], 32000136: ['LY', 'Libya'], 32000137: ['LI', 'Liechtenstein'], 32000138: ['LT', 'Lithuania'], 32000139: ['LU', 'Luxembourg'], 32000140: ['MO', 'Macau'], 32000141: ['MK', 'North Macedonia'], 32000142: ['MG', 'Madagascar'], 32000143: ['MW', 'Malawi'], 32000144: ['MY', 'Malaysia'], 32000145: ['MV', 'Maldives'], 32000146: ['ML', 'Mali'], 32000147: ['MT', 'Malta'], 32000148: ['MH', 'Marshall Islands'], 32000149: ['MQ', 'Martinique'], 32000150: ['MR', 'Mauritania'], 32000151: ['MU', 'Mauritius'], 32000152: ['YT', 'Mayotte'], 32000153: ['MX', 'Mexico'], 32000154: ['FM', 'Micronesia'], 32000155: ['MD', 'Moldova'], 32000156: ['MC', 'Monaco'], 32000157: ['MN', 'Mongolia'], 32000158: ['ME', 'Montenegro'], 32000159: ['MS', 'Montserrat'], 32000160: ['MA', 'Morocco'], 32000161: ['MZ', 'Mozambique'], 32000162: ['MM', 'Myanmar (Burma)'], 32000163: ['NA', 'Namibia'], 32000164: ['NR', 'Nauru'], 32000165: ['NP', 'Nepal'], 32000166: ['NL', 'Netherlands'], 32000167: ['NC', 'New Caledonia'], 32000168: ['NZ', 'New Zealand'], 32000169: ['NI', 'Nicaragua'], 32000170: ['NE', 'Niger'], 32000171: ['NG', 'Nigeria'], 32000172: ['NU', 'Niue'], 32000173: ['NF', 'Norfolk Island'], 32000174: ['KP', 'North Korea'], 32000175: ['MP', 'Northern Mariana Islands'], 32000176: ['NO', 'Norway'], 32000177: ['OM', 'Oman'], 32000178: ['PK', 'Pakistan'], 32000179: ['PW', 'Palau'], 32000180: ['PS', 'Palestine'], 32000181: ['PA', 'Panama'], 32000182: ['PG', 'Papua New Guinea'], 32000183: ['PY', 'Paraguay'], 32000184: ['PE', 'Peru'], 32000185: ['PH', 'Philippines'], 32000186: ['PN', 'Pitcairn Islands'], 32000187: ['PL', 'Poland'], 32000188: ['PT', 'Portugal'], 32000189: ['PR', 'Puerto Rico'], 32000190: ['QA', 'Qatar'], 32000191: ['RE', 'Réunion'], 32000192: ['RO', 'Romania'], 32000193: ['RU', 'Russia'], 32000194: ['RW', 'Rwanda'], 32000195: ['BL', 'Saint Barthélemy'], 32000196: ['SH', 'Saint Helena'], 32000197: ['KN', 'Saint Kitts and Nevis'], 32000198: ['LC', 'Saint Lucia'], 32000199: ['MF', 'Saint Martin'], 32000200: ['PM', 'Saint Pierre and Miquelon'], 32000201: ['WS', 'Samoa'], 32000202: ['SM', 'San Marino'], 32000203: ['ST', 'São Tomé and Príncipe'], 32000204: ['SA', 'Saudi Arabia'], 32000205: ['SN', 'Senegal'], 32000206: ['RS', 'Serbia'], 32000207: ['SC', 'Seychelles'], 32000208: ['SL', 'Sierra Leone'], 32000209: ['SG', 'Singapore'], 32000210: ['SX', 'Sint Maarten'], 32000211: ['SK', 'Slovakia'], 32000212: ['SI', 'Slovenia'], 32000213: ['SB', 'Solomon Islands'], 32000214: ['SO', 'Somalia'], 32000215: ['ZA', 'South Africa'], 32000216: ['KR', 'South Korea'], 32000217: ['SS', 'South Sudan'], 32000218: ['ES', 'Spain'], 32000219: ['LK', 'Sri Lanka'], 32000220: ['VC', 'St. Vincent & Grenadines'], 32000221: ['SD', 'Sudan'], 32000222: ['SR', 'Suriname'], 32000223: ['SJ', 'Svalbard and Jan Mayen'], 32000224: ['SZ', 'Swaziland'], 32000225: ['SE', 'Sweden'], 32000226: ['CH', 'Switzerland'], 32000227: ['SY', 'Syria'], 32000228: ['TW', 'Taiwan'], 32000229: ['TJ', 'Tajikistan'], 32000230: ['TZ', 'Tanzania'], 32000231: ['TH', 'Thailand'], 32000232: ['TL', 'Timor-Leste'], 32000233: ['TG', 'Togo'], 32000234: ['TK', 'Tokelau'], 32000235: ['TO', 'Tonga'], 32000236: ['TT', 'Trinidad and Tobago'], 32000237: ['TA', 'Tristan da Cunha'], 32000238: ['TN', 'Tunisia'], 32000239: ['TR', 'Turkey'], 32000240: ['TM', 'Turkmenistan'], 32000241: ['TC', 'Turks and Caicos Islands'], 32000242: ['TV', 'Tuvalu'], 32000243: ['UM', 'U.S. Outlying Islands'], 32000244: ['VI', 'U.S. Virgin Islands'], 32000245: ['UG', 'Uganda'], 32000246: ['UA', 'Ukraine'], 32000247: ['AE', 'United Arab Emirates'], 32000248: ['GB', 'United Kingdom'], 32000249: ['US', 'United States'], 32000250: ['UY', 'Uruguay'], 32000251: ['UZ', 'Uzbekistan'], 32000252: ['VU', 'Vanuatu'], 32000253: ['VA', 'Vatican City'], 32000254: ['VE', 'Venezuela'], 32000255: ['VN', 'Vietnam'], 32000256: ['WF', 'Wallis and Futuna'], 32000257: ['EH', 'Western Sahara'], 32000258: ['YE', 'Yemen'], 32000259: ['ZM', 'Zambia'], 32000260: ['ZW', 'Zimbabwe']}


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

@app.get("/legends/{player_tag}/{date}")
@limiter.limit("30/second")
async def legends(player_tag: str, date: str,request : Request, response: Response):
    player_tag = coc.utils.correct_tag(player_tag)
    results = await player_stats.find_one({"tag" : player_tag})
    if results is None:
        return {"Player not tracked"}
    legends = results.get("legends")
    if legends is None:
        return {"tag" : player_tag,
            "attacks" : [],
            "num_attack" : 0,
            "defenses" : [],
            "streak" : 0,
            "global_rank" : None,
            "local_rank" : None,
            "country_name" : None,
            "country_code" : None
            }
    date_legends = legends.get(f"{date}")
    if date_legends is None:
         return {"tag" : player_tag,
            "attacks" : [],
            "num_attack" : 0,
            "defenses" : [],
            "streak" : 0,
            "global_rank" : None,
            "local_rank" : None,
            "country_name" : None,
            "country_code" : None
            }
    num_attacks = date_legends.get("num_attacks")
    if num_attacks is None:
        num_attacks = 0
    attacks = date_legends.get("attacks")
    defenses = date_legends.get("defenses")
    streak = legends.get("streak")
    if attacks is None:
        attacks = []
    if defenses is None:
        defenses = []
    if streak is None:
        streak = 0

    country_name = results.get("country_name")
    country_code = results.get("country_code")

    try:
        local_ranking = LOCATION_CACHE[player_tag]
        local_rank = local_ranking[1]
    except:
        local_rank = None

    try:
        global_ranking = GLOBAL_LOCATION_CACHE[player_tag]
        global_rank = global_ranking
    except:
        global_rank = None

    return {"tag" : player_tag,
            "attacks" : attacks,
            "num_attack" : num_attacks,
            "defenses" : defenses,
            "streak" : streak,
            "global_rank" : global_rank,
            "local_rank" : local_rank,
            "country_name" : country_name,
            "country_code" : country_code
            }

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

@app.websocket("/wars")
async def clan_websocket(websocket: WebSocket, token: str = Query(...), Authorize: AuthJWT = Depends()):
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

    while True:

        try:
            rtime = time.time()
            clan_tags = await user_db.distinct("tracked_clans")
            global keys

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
                from jsondiff import diff
                if str(previous_response) != str(response):
                    CLAN_CACHE[tag] = response
                    for ws in CLAN_CLIENTS:
                        ws: fastapi.WebSocket
                        if diff(response, previous_response) is not None:
                            differences = diff(response, previous_response)
                            for dif in differences:
                                if "$delete" not in str(dif):
                                    json_data = {"type": dif, "old_clan": previous_response,
                                                 "new_clan": response}
                                    if previous_response is not None:
                                        await ws.send_json(json_data)

            print(f"CLAN RESPONSES: {time.time() - rtime}")

            db_tags = await player_stats.distinct("tag")
            all_tags_to_track = list(set(db_tags + CLAN_MEMBERS))
            print(f"{len(all_tags_to_track)} tags")

            #GENERATE DATES FOR STAT COLLECTION
            raid_date = gen_raid_date()
            season = gen_season_date()
            legend_date = gen_legend_date()


            '''
            # WAR EVENTS
            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.read()
                    return player_

            tasks = []
            connector = aiohttp.TCPConnector(limit=1000)
            async with aiohttp.ClientSession(connector=connector) as session2:
                for tag in clan_tags:
                    headers = {"Authorization": f"Bearer {keys[0]}"}
                    tag = tag.replace("#", "%23")
                    url = f"https://api.clashofclans.com/v1/clans/{tag}/currentwar"
                    keys = collections.deque(keys)
                    keys.rotate(1)
                    keys = list(keys)
                    task = asyncio.ensure_future(fetch(url, session2, headers))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                await session2.close()


            from jsondiff import diff
            for response in responses:
                response = orjson.loads(response)
                try:
                    tag = response["clan"]["tag"]
                except:
                    continue

                try:
                    previous_response = WAR_CACHE[tag]
                except:
                    previous_response = None
                if diff(response, previous_response) is not None and str(diff(response, previous_response)) != "{}":
                    #print(f"{tag} : {diff(response, previous_response)}")
                    pass
                if str(previous_response) != str(response):
                    WAR_CACHE[tag] = response
                    for ws in WAR_CLIENTS:
                        ws: fastapi.WebSocket
                        if diff(response, previous_response) is not None:
                            differences = diff(response, previous_response)
                            print(differences)
                            for dif in differences:
                                if str(dif) == "clan":
                                    continue
                                    
                                    for ach in differences["achievements"]:
                                        name = response['achievements'][int(ach)]['name']
                                        json_data = {"type": name, "old_player": previous_response,
                                                     "new_player": response}
                                        if previous_response is not None:
                                            await ws.send_json(json_data)
                                    
                                else:
                                    if "$delete" not in str(dif):
                                        json_data = {"type": dif, "old_war": previous_response,
                                                     "new_war": response}
                                        if previous_response is not None:
                                            await ws.send_json(json_data)
            '''

            #tag_list = divide_chunks(all_tags_to_track, 25000)
            #for t_list in tag_list:
            #PLAYER EVENTS
            async def fetch(url, session, headers):
                async with session.get(url, headers=headers) as response:
                    player_ = await response.json(loads=orjson.loads)
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
                KEPT_ACHIEVEMENTS = ["Gold Grab", "Elixir Escapade", "Games Champion", "Aggressive Capitalism", "Most Valuable Clanmate"]
                try:
                    #response = orjson.loads(response)
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
                #changes.append(UpdateOne({"tag": tag}, {"$set": {"league": league}}))
                if previous_response is not None:
                    if str(previous_response) != str(response):
                        #from jsondiff import diff
                        pc_copy =PLAYER_CLIENTS.copy()
                        '''
                        for ws in pc_copy:
                            ws: fastapi.WebSocket
                            if diff(response, previous_response) is not None:
                                differences = diff(response, previous_response)
                                list_diff = []
                                for dif in differences:
                                    if str(dif) == "clan":
                                        continue
                                    if str(dif) == "achievements":
                                        for ach in differences["achievements"]:
                                            name = response['achievements'][int(ach)]['name']
                                            list_diff.append(name)
                                    else:
                                        if "$delete" not in str(dif):
                                            list_diff.append(dif)
                                json_data = {"type": list_diff, "old_player": previous_response, "new_player" : response}
                                if previous_response is not None:
                                    try:
                                        #await
                                        task = asyncio.ensure_future(ws.send_json(json_data))
                                        ws_tasks.append(task)
                                    except:
                                        PLAYER_CLIENTS.remove(ws)
                                        continue
                            '''
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
                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"capital_gold.{raid_date}.donate": diff}}))
                            for ws in pc_copy:
                                ws: fastapi.WebSocket
                                # await
                                json_data = {"type": "Most Valuable Clanmate", "old_player": previous_response,"new_player": response}
                                task = asyncio.ensure_future(ws.send_json(json_data))
                                ws_tasks.append(task)
                        # CAPITAL GOLD RAID CHANGE
                        if response["achievements"][-2]["value"] != previous_response["achievements"][-2]["value"]:
                            diff = response["achievements"][-2]["value"] - previous_response["achievements"][-2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$push": {f"capital_gold.{raid_date}.raid": diff}}))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"capital_gold.{raid_date}.raided_clan": clan_tag}}))
                            for ws in pc_copy:
                                ws: fastapi.WebSocket
                                # await
                                json_data = {"type": "Aggressive Capitalism", "old_player": previous_response,"new_player": response}
                                task = asyncio.ensure_future(ws.send_json(json_data))
                                ws_tasks.append(task)
                        #CLAN GAME CHANGE
                        if response["achievements"][2]["value"] != previous_response["achievements"][2]["value"]:
                            diff = response["achievements"][2]["value"] - previous_response["achievements"][2]["value"]
                            changes.append(UpdateOne({"tag": tag}, {"$inc": {f"clan_games.{season}.points": diff}}))
                            changes.append(UpdateOne({"tag": tag}, {"$set": {f"clan_games.{season}.clan": clan_tag}}))

                        # NAME CHANGE
                        if response["name"] != previous_response["name"]:
                            changes.append(UpdateOne({"tag": tag}, {"$set": {"name": response["name"]}}))

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
                                    task = asyncio.ensure_future(ws.send_json(json_data))
                                    ws_tasks.append(task)

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

            print(f"PLAYER RESPONSE DONE: {time.time() - rtime}")

            responses = await asyncio.gather(*ws_tasks, return_exceptions=False)

            print(f"SENT ALL WS: {time.time() - rtime}")

            #LEADERBOARD CACHE
            async def fetch(url, session, headers, location):
                async with session.get(url, headers=headers) as response:
                    location_lb = await response.read()
                    return [location, location_lb]

            tasks = []
            connector = aiohttp.TCPConnector(limit=100)
            async with aiohttp.ClientSession(connector=connector) as session4:
                for location in LOCATIONS:
                    headers = {"Authorization": f"Bearer {keys[0]}"}
                    url = f"https://api.clashofclans.com/v1/locations/{location}/rankings/players"
                    keys = collections.deque(keys)
                    keys.rotate(1)
                    keys = list(keys)
                    task = asyncio.ensure_future(fetch(url, session4, headers, location))
                    tasks.append(task)
                responses = await asyncio.gather(*tasks)
                await session4.close()


            print(f"FETCH LB: {time.time() - rtime}")
            await leaderboard_db.update_many({}, {"$set" : {"global_rank" : None, "local_rank" : None}})
            print(f"UNSETTING ALL: {time.time() - rtime}")
            lb_changes = []

            for response in responses:
                location = response[0]
                if location != "global":
                    location_data = LOCATION_DATA[location]
                    location_code = location_data[0]
                    location_name = location_data[1]
                full_lb = response[1]
                full_lb = orjson.loads(full_lb)
                for _player in full_lb["items"]:
                    tag = _player["tag"]
                    if location == "global":
                        lb_changes.append(UpdateOne({"tag": _player["tag"]}, {"$set": {f"global_rank": _player["rank"]}}, upsert=True))
                    else:
                        lb_changes.append(UpdateOne({"tag": _player["tag"]}, {"$set": {f"local_rank": _player["rank"], f"country_name": location_name, f"country_code": location_code}}, upsert=True))


            print(f"LB CHANGES: {time.time() - rtime}")
            if changes != []:
                results = await player_stats.bulk_write(changes)
                print(results.bulk_api_result)
                print(f"CHANGES INSERT: {time.time() - rtime}")
            if lb_changes != []:
                results = await leaderboard_db.bulk_write(lb_changes)
                print(results.bulk_api_result)
                print(f"LB INSERT: {time.time() - rtime}")

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


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

loop = asyncio.get_event_loop()
config = Config(app=app, loop="asyncio", host = "104.251.216.53", port=8000,ws_ping_interval=None ,ws_ping_timeout= None)
server = Server(config)
loop.create_task(server.serve())
loop.create_task(broadcast())
loop.run_forever()


