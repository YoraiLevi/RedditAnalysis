import ujson

base_data = {key:None for key in fields}
def process_line(line):
    obj = ujson.decode(line)
    data = base_data.copy()
    # data = defaultdict(None)
    data["retrieved_utc"] = obj.get("retrieved_on")
    data.update({key: obj[key] for key in obj if key in fields})
    if isinstance(data["created_utc"], str):
        data["created_utc"] = int(data["created_utc"])

    left_over = {key: obj[key] for key in obj if key not in fields}

    data["json"] = ujson.encode(left_over)
    # print(data["json"])
    return dict(data)
with open("C:\Users\devic\OneDrive\Documents\Datasets\reddit\comments\RC_2005-12.ndjson") as f:
    for line in f.readlines():
        print(ujson.encode(process_line(line)))
