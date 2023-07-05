from gbfs.services import SystemDiscoveryService
import pandas as pd
import numpy as np
import time
from geopy.distance import great_circle
from tqdm import tqdm
import pickle
import datetime

ds = SystemDiscoveryService()

clients = {}

for x in ds.systems:
    if 'Seattle' in x.get('Location') or 'Los Angeles' in x.get('Location'):
        clients[x["System ID"]] = ds.instantiate_client(x["System ID"])

print(clients)

bikes = {client:{} for client in clients}

i = 1
while(True):
    if i % 120 == 0:
        # save data every 2h:
        timestamp = str(f"{datetime.datetime.now():%Y-%m-%d_%H}")
        with open(f'data/bikes_{timestamp}.pickle', 'wb') as f:
            pickle.dump(bikes, f)
        print(sum(len(bikes[c][f]) for c in bikes for f in bikes[c]))
        bikes = {client:{} for client in clients}
    
    for client in clients:
        try:
            result = clients[client].request_feed('free_bike_status')   
            timestamp = result["last_updated"]
            
            for bike in result["data"]["bikes"]:
                bike_id = bike["bike_id"]
                tmp = bikes[client].get(bike_id, [])
                if len(tmp) > 0:
                    last = tmp[-1]
                    if great_circle((bike["lat"],bike["lon"]), 
                                    (last[1], last[2])).meters > 100 or \
                        last[3] != bike["is_reserved"] or \
                        last[4] != bike["is_disabled"] :
                        
                        tmp.append((timestamp, 
                                    np.round(bike["lat"],5), 
                                    np.round(bike["lon"],5), 
                                    bike['is_reserved'], bike['is_disabled'],))
                        bikes[client][bike_id] = tmp
                else:
                    tmp.append((timestamp, 
                                np.round(bike["lat"]), 
                                np.round(bike["lon"]), 
                                bike['is_reserved'], bike['is_disabled'],))
                    bikes[client][bike_id] = tmp
        except Exception as e:
        	print(str(e))
            pass
    time.sleep(60)
    i += 1
    
# last save:
timestamp = str(f"{datetime.datetime.now():%Y-%m-%d_%H}")
print(sum(len(bikes[c][f]) for c in bikes for f in bikes[c]))
with open(f'data/bikes_{timestamp}.pickle', 'wb') as f:
    pickle.dump(bikes, f)