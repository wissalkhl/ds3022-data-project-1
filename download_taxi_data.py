import os
import requests

# Make sure the data folder exists
os.makedirs("data", exist_ok=True)

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
months = [f"{m:02d}" for m in range(1, 13)]
colors = ["yellow", "green"]

for color in colors:
    for month in months:
        filename = f"{color}_tripdata_2024-{month}.parquet"
        url = base_url + filename
        out_path = os.path.join("data", filename)
        print(f"Downloading {url} -> {out_path}")

        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            print(f"Failed to download {url} (status {r.status_code})")

            
