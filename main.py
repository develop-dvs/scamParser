import csv
import threading
import time

import requests
from tqdm import tqdm

OUTPUT_DIR = "data"
THREADS_COUNT = 100
BASE_URL = "https://saverudata.info/"


def fetch_csv(url):
    i = 0
    while i < 3:
        try:
            res = requests.get(url)
            return res.content.decode('utf-8')
        except Exception as err:
            print(f"Error to fetch  {url} {err=}, {type(err)=}")
            i += 1
            time.sleep(1)
    return f"Error to fetch {url}"


def get_csv_data(file):
    with open(file) as geohash:
        reader = csv.reader(geohash, delimiter=',')
        data = list(reader)
        chunk = len(data) // THREADS_COUNT
        return list(zip(*[iter(data)] * chunk))

def handle_chunk(pos, chunk):
    text = f"Fetch #{pos}"
    with tqdm(total=len(chunk), position=pos, desc=text) as progress:
        for g_str in chunk:
            # /db/dbgeo/s/z/g/szgcye.csv
            url = f"{BASE_URL}db/dbgeo/{g_str[0][0:1]}/{g_str[0][1:2]}/{g_str[0][2:3]}/{g_str[0]}.csv"
            with open(f"{OUTPUT_DIR}/{g_str[0]}.csv", "w+", encoding="utf-8") as out:
                try:
                    response = fetch_csv(url)
                    out.write(response)
                except Exception:
                    out.write(f"Error to fetch {url}")
            progress.update(1)


def main():
    # download latest geohash base
    url = f"{BASE_URL}map/geohash.csv"
    with open(f"geohash.csv", "w+", encoding="utf-8") as out:
        try:
            response = fetch_csv(url)
            out.write(response)
        except Exception as err:
            print(f"Error to fetch  {url} {err=}, {type(err)=}")
            out.write(f"Error to fetch {url}")

    # open saved csv
    data = get_csv_data("geohash.csv")

    for pos, chunk in enumerate(data, 1):
        try:
            thread = threading.Thread(target=handle_chunk, args=(pos, chunk))
            thread.start()
        except RuntimeError:
            continue


if __name__ == "__main__":
    main()
