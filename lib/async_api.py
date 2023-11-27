from itertools import chain
import aiohttp
import asyncio
from dateutil.relativedelta import relativedelta
from config.api_config import URL, TOKEN


class Api:
    def __init__(self):
        self.url = URL
        self.headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'x-access-token': TOKEN,
        }

    async def fetch(self, session, request): 
        print('request', request['url'])
        async with session.get(request['url'], params=request['params'], headers=self.headers) as response:
            if response.status == 200:
                request['json'] = await response.json() 
                print(f"Got result of {request['url']}")
                request['result'] = response.status
                request['status'] = 'done'
            elif response.status == 429:
                print(f"error: too many requests {response.status}")
                print("scheduling request again")
                request['result'] = None
                request['status'] = 'new'
            else: 
                print(f"error {response.status}")
                request['result'] = None
                request['status'] = f"failed {response.status}"
          


    async def fetch_all(self, session, urls, persecond):
        requests = [{
            'url': url,
            'result': None,
            'status': 'new',
            'json': None,
            'params': {}
        } for url in urls]
        counter = 0 
        while True:
            running_tasks = len([i for i in requests if i['status'] in ['fetch']])
            is_tasks_to_wait = len([i for i in requests if i['status'] != 'done'])

            if counter < len(requests) and running_tasks < persecond:
                requests[n]['status'] = 'fetch'
                asyncio.create_task(self.fetch(session, requests[counter]))
                n += 1
                print(f'Schedule tasks {counter}. '
                    f'Running {running_tasks} '
                    f'Remain {is_tasks_to_wait}')
            if running_tasks >= persecond:
                print('Throttling')
                await asyncio.sleep(1)
            if is_tasks_to_wait != 0:
                await asyncio.sleep(0.1)
            else:
                # All tasks done
                break
        return requests
    


    
    async def get_detections(self, counter_id, months):
        print(f'get detection for {counter_id}, from {months[0]} to {months[-1]}')
        
        urls = [self.get_detection_url(counter_id, month) for month in months]

        async with aiohttp.ClientSession() as session:
            responses = await self.fetch_all(session, urls, 10)
            detections = []
            for month, response in zip(months, responses):
                
                if response["status"] == "done":
                    print(f"Successfully fetched {month}")
                    data = response['json']
                    if (not data=={}):
                        detections.append(list(map(self.__map_detection, data)))
                else:
                    print(f"Failed to fetch {month}")
            return detections

    def get_detection_url(self, counter_id, month):
            next_month = month + relativedelta(months=+1)
            return f'{self.url}bicyclecounters/detections?id={counter_id}&limit=10000,&offset=0&from={month}&to={next_month}&aggregate=false'


    async def get_counters(self):
        params = {
            'latlng': '50.124935,14.457204',
            'range': 500000,
            'limit': 50,
            'offset': 0,
        }

        request = {
            'url': self.url + 'bicyclecounters',
            'result': None,
            'status': 'new', 
            'json': None,
            'params': params
        }       

        async with aiohttp.ClientSession() as session:
            await asyncio.create_task(self.fetch(session, request))
            data = request['json']
            if (data=={}):
                return []
            features = data["features"]
            nested_counters = list(map(self.__map_counter, features))
            return list(chain(*nested_counters))


    #  map functions
    def __map_counter(self, x):
        prop = x['properties']
        counters = []
        for direction in prop['directions']:
            counters.append({
                'location_id': prop['id'],
                'name': prop['name'],
                'route': prop['route'],
                'direction_id': direction['id'],
                'direction_name': direction['name'],
            })
        return counters

    def __map_detection(self, prop):
        return {
             'counter_direction_id': prop['id'],
             'time': prop['measured_from'],
             'bikes': prop['value'],
             'pedestrians': prop['value_pedestrians'],
        }
