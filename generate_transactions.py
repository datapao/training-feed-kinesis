import json, sys
import datetime, time, random

while True:
    t = {
        'timestamp': datetime.datetime.now().isoformat(),
        'int': random.randint(0, 1000),
        'string': ['HU', 'US'][random.randint(0,1)]
    }
    print(json.dumps(t))
    time.sleep(0.1)
