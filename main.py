import aiofiles
import argparse
import asyncio
from contextlib import asynccontextmanager
import threading

from vkbottle import API


TASKS_LIMIT = 10
API_ERROR_THRESHOLD = 10
fields = ','.join(["education", "universities", "schools", "status", "last_seen"])

shared_int = 0
shared_int_lock = threading.Lock()


@asynccontextmanager
async def api_semaphore(api, sem):
    await sem.acquire()
    try:
        yield api
    finally:
        sem.release()


class CustomAPI:
    def __init__(self, token, number):
        self.api = API(token=token)
        self.token = token
        self.number = number

    def get_info(self):
        return str({self.number})

    def __getattr__(self, name):
        return getattr(self.api, name)


def get_api(token, number):
    api = CustomAPI(token, number)
    return api


def read_lines(file_name):
    res = []
    with open(file_name, 'r') as file:
        for line in file:
            res.append(line[:-1])
    return res


async def send_request(api_request, api):
    try:
        fst, snd = api_request
        response = await api.request(fst, snd)
        return response
    except Exception as e:
        return None


async def get_users(users_ids, apis, fields=''):
    user_list = ','.join([str(x) for x in users_ids])
    api_request = ("users.get", {"user_ids": user_list, "fields": fields})
    response = await send_request(api_request, apis)
    if response is None:
        return []
    return response['response']


async def get_wall(user_id, api, limit=10):
    api_request = ("wall.get", {'owner_id': user_id, 'offset': 0, 'count': limit})
    response = await send_request(api_request, api)
    if response is None:
        return []
    return response['response']


async def a_write(file, data):
    async with aiofiles.open(file, 'a') as file:
        await file.write(data)


def update_shared_int(upd):
    global shared_int
    with shared_int_lock:
        shared_int += upd


async def process_users(queue, api, sem, file):
    global shared_int
    error_counter = 0
    # shared_int needs to catch situations when user_id is picked but not processed due to invalid token,
    # we need not stop other workers
    while (not queue.empty() or shared_int > 0) and error_counter < API_ERROR_THRESHOLD:
        if queue.empty():
            await asyncio.sleep(0)
            continue
        user_id = await queue.get()
        update_shared_int(1)
        async with api_semaphore(api, sem):
            try:
                user_info = await get_users([user_id], api, fields)
                user_wall = await get_wall(user_id, api)
                output = f"User Info: {user_info} Wall: {user_wall}\n"
                await a_write(file, output)
            except Exception as e:
                error_counter += 1
                queue.put_nowait(user_id)
            update_shared_int(-1)

    if error_counter == API_ERROR_THRESHOLD:
        print(f"Token {api.get_info()} has reached the error threshold.")


def parse_args():
    parser = argparse.ArgumentParser(description="Simple concurrent VK API parser")

    parser.add_argument("--tokens_path", type=str, required=True, help="Path with access tokens to VK API")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--user_ids", type=lambda s: [int(item) for item in s.split(',')],
                       help="List of user ids separated by commas")
    group.add_argument("--user_ids_path", type=str, help="Path to the file containing user ids")

    parser.add_argument("--output_path", type=str, required=True, help="Path where results will be stored")

    return parser.parse_args()


async def main():
    args = parse_args()
    tokens = read_lines(args.tokens_path)
    api_list = [get_api(token, i) for i, token in enumerate(tokens)]
    users_ids_to_process = args.user_ids if args.user_ids else read_lines(args.user_ids_path)

    queue = asyncio.Queue()
    for user_id in users_ids_to_process:
        queue.put_nowait(user_id)
    # limit simultaneous API usage
    sem = asyncio.Semaphore(min(len(api_list), TASKS_LIMIT))
    tasks = []

    for api in api_list:
        tasks.append(asyncio.create_task(process_users(queue, api, sem, args.output_path)))
    await asyncio.gather(*tasks)

    print(f"Program finished, info dumped to {args.output_path}")


asyncio.run(main())
