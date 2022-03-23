import json
import threading
import time

import requests
from tqdm import tqdm

OUTPUT_DIR = "data"
THREADS_COUNT = 1000
BASE_URL = "https://saverudata.org/dbgeo/"


def fetch_json(url):
    i = 0
    while i < 3:
        try:
            res = requests.get(url)
            return res.json()
        except Exception:
            i += 1
            time.sleep(1)
    return f"Error to fetch {url}"


def get_json_data(file):
    with open(file) as geohash:
        data = json.load(geohash)
        chunk = len(data) // THREADS_COUNT
        return list(zip(*[iter(data)] * chunk))


def handle_chunk(*args):
    for g_str in tqdm(args):
        url = f"{BASE_URL}{g_str[0:1]}/{g_str[1:2]}/{g_str[2:3]}/{g_str}.json"
        response = fetch_json(url)
        with open(f"{OUTPUT_DIR}/{g_str}.json", "w+", encoding="utf-8") as out:
            out.write(json.dumps(response, ensure_ascii=False))


def main():
    data = get_json_data("geohash.json")

    for thread_chunk in data:
        try:
            thread = threading.Thread(
                target=handle_chunk, args=thread_chunk, daemon=True
            )
            thread.start()
        except RuntimeError:
            continue


if __name__ == "__main__":
    main()
