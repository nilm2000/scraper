import os, json, requests, sys
import pandas as pd

try:
  from homeharvest import scrape_property
except Exception as e:
  print("Failed to import homeharvest:", e, file=sys.stderr)
  raise

ANVIL_INGEST_URL  = os.environ.get("ANVIL_INGEST_URL")
INGEST_SECRET     = os.environ.get("INGEST_SECRET")
ANVIL_PRESETS_URL = os.environ.get("ANVIL_PRESETS_URL")  # optional

def load_presets():
  if ANVIL_PRESETS_URL:
    r = requests.get(ANVIL_PRESETS_URL, headers={"X-INGEST-KEY": INGEST_SECRET}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data.get("presets", [])
  # fallback: local presets.json
  with open("presets.json", "r", encoding="utf-8") as f:
    return json.load(f)

def fetch_one(p):
  kwargs = dict(
    location=p["location"],
    listing_type=p.get("listing_type","for_sale"),
    past_days=int(p.get("past_days", 7)),
    exclude_pending=bool(p.get("exclude_pending", True)),
    return_type="pandas",
  )
  if p.get("property_type") and p["property_type"] != "any":
    kwargs["property_type"] = [p["property_type"]]
  print("Scraping:", kwargs)
  try:
    return scrape_property(**kwargs)
  except Exception as e:
    print("Scrape error:", e, file=sys.stderr)
    return pd.DataFrame()

def df_to_items(df: pd.DataFrame):
  if df is None or df.empty: return []
  keep = {"property_url","property_id","listing_id","mls_id",
          "formatted_address","city","state","zip_code","list_price","list_date"}
  items = []
  for _, r in df.iterrows():
    d = {k: (r[k] if k in r else None) for k in keep}
    # normalize text fields to strings
    for t in ["property_url","property_id","listing_id","mls_id",
              "formatted_address","city","state","zip_code","list_date"]:
      if t in d and d[t] is not None:
        d[t] = str(d[t])
    items.append(d)
  return items

def push_items(items, source="realtor"):
  if not items:
    return {"received": 0, "inserted": 0, "updated": 0}
  resp = requests.post(
    ANVIL_INGEST_URL,
    headers={"Content-Type":"application/json","X-INGEST-KEY":INGEST_SECRET},
    data=json.dumps({"source": source, "items": items}),
    timeout=120
  )
  resp.raise_for_status()
  return resp.json()

def main():
  if not ANVIL_INGEST_URL or not INGEST_SECRET:
    print("Missing ANVIL_INGEST_URL or INGEST_SECRET", file=sys.stderr)
    sys.exit(2)
  presets = load_presets()
  totals = {"received":0,"inserted":0,"updated":0}
  for p in presets:
    df = fetch_one(p)
    items = df_to_items(df)
    res = push_items(items)
    print("Pushed:", res)
    for k in totals: totals[k] += res.get(k, 0)
  print("DONE:", totals)

if __name__ == "__main__":
  main()

