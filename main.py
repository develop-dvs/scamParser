import json
import threading
import time

import requests
from tqdm import tqdm

OUTPUT_DIR = "data"
THREADS_COUNT = 100
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


def handle_chunk(pos, chunk):
    text = f"Fetch #{pos}"
    with tqdm(total=len(chunk), position=pos, desc=text) as progress:
        for g_str in chunk:
            url = f"{BASE_URL}{g_str[0:1]}/{g_str[1:2]}/{g_str[2:3]}/{g_str}.json"
            with open(f"{OUTPUT_DIR}/{g_str}.json", "w+", encoding="utf-8") as out:
                try:
                    response = fetch_json(url)
                    out.write(json.dumps(response, ensure_ascii=False))
                except Exception:
                    out.write(f"Error to fetch {url}")
            progress.update(1)


def main():
    data = get_json_data("geohash.json")

    for pos, chunk in enumerate(data, 1):
        try:
            thread = threading.Thread(target=handle_chunk, args=(pos, chunk))
            thread.start()
        except RuntimeError:
            continue


if __name__ == "__main__":
    main()
