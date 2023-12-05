"""
The script downloads detections for the specified counter and time.

Input arguments: 
    1: counter direction id (e.g. camea-BC_CL-PV) 
    2: Starting month. 
    3. Number of months to be downloaded
"""

from lib.async_api import Api
import asyncio
from datetime import datetime
from lib.db import Db
from dateutil.relativedelta import relativedelta
import sys

async def get_detections(counter_id, start_date, months_count):
    db = Db()
    api = Api()

    months = []
    next_month = start_date;
    for _ in range(months_count):
        months.append(next_month)
        next_month = next_month + relativedelta(months=+1)            

    print('Getting detections')
    detections = await api.get_detections(counter_id, months)
    print(f'{len(detections)} detection records downloaded')
    

    if (not db.table_exists('detection')):
        print('Creating detection table')
        db.create_detection_table()
    else: 
        print('Detection table already exist')


    print('Inserting detections to database')
    db.insert_df(detections, 'detection')

    print('finished')

if __name__=="__main__":

    id = None
    start = None
    count = None

    # getting command line arguments
    # todo - add more argument validation
    if (len(sys.argv) > 1):
        id = sys.argv[1]
    else:
        print('add counter id as a first agumment')
        sys.exit()

    if (len(sys.argv) > 2):
        date_str = sys.argv[2]
        start = datetime.strptime(date_str, '%d.%m.%Y')
    else:
        print('add date as a second agumment')
        sys.exit()

    if (len(sys.argv) > 3):
        count = int(sys.argv[3])
    else:
        print('add month count as a third argument')
        sys.exit()

    # run async script
    asyncio.run(get_detections(id, start, count))
