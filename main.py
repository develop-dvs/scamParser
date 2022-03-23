import json
import time

import requests
from tqdm import tqdm

OUTPUT_DIR = "data"


def fetch_json(url):
    i = 0
    while i < 3:
        try:
            res = requests.get(url)
            return res.json()
        except Exception:
            i += 1
            time.sleep(1)
    return ""


with open("geohash.json") as geohash:
    data = json.load(geohash)
    for hash_string in tqdm(data):
        response = fetch_json(
            f"https://saverudata.org/dbgeo/{hash_string[0:1]}/{hash_string[1:2]}/{hash_string[2:3]}/{hash_string}.json"
        )
        with open(
            f"{OUTPUT_DIR}/{hash_string}.json", "w+", encoding="utf-8"
        ) as output_json:
            output_json.write(json.dumps(response, ensure_ascii=False))

import threading
import time


def mythread():
    print("1")


def main():
    threads = 0  # thread counter
    y = 1000  # a MILLION of 'em!
    for i in range(y):
        try:
            x = threading.Thread(target=mythread, daemon=True)
            threads += 1  # thread counter
            x.start()  # start each thread
        except RuntimeError:  # too many throws a RuntimeError
            break
    print("{} threads created.\n".format(threads))


if __name__ == "__main__":
    main()
