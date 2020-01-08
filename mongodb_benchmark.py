import pymongo
import ssl
import time
import multiprocessing as mp
import argparse
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Configuration
MONGODB_HOSTS = [
    'mongodb01.example.com',
    'mongodb02.example.com',
    'mongodb03.example.com'
]
cluster_name = 'Test' # Replicaset Name
password = 'test123'  # Password
username = 'test'    # Username

doc = {"state": "CT", "postcode": "06037", "street": "Parish Dr",
    "district": "", "unit": "",
    "location": {"type": "Point", "coordinates": [-72.7738706, 41.6332836]},
    "region": "Hartford", "number": "51", "city": "Berlin"}

client = pymongo.MongoClient(
    MONGODB_HOSTS,
    ssl=True,
    ssl_cert_reqs=ssl.CERT_NONE,
    username=username,
    password=password,
    replicaset=cluster_name,
    w=2,
    connect=False
)

def test_mongodb():
    if client.is_primary:
        db = client.test
        m = db.client.address
        print('\t\x1b[6;30;42m' + 'Connecting to current MongoDB (Master) :\x1b[0m {}'.format(
                m[0]))
        print('\x1b[6;30;43m' + 'Creating document' + '\x1b[0m')
        print(db.test.insert_one(doc).inserted_id)
        print('\x1b[6;30;43m' + 'Reading document' + '\x1b[0m')
        print(db.test.find_one())
        print('\x1b[6;30;43m' + 'Deleting document' + '\x1b[0m')
        r = db.test.delete_many(doc)
        print(r.acknowledged)

def benchmark(n):
    start = time.time()
    i = 0
    while (i < n):
        excute_query(i)
        i = i + 1
    end = time.time()
    totalTime = (end - start) * 1000
    print('Total = {} ms'.format(totalTime))

def excute_query(i):
    start = time.time()
    client.tests.insertTest.insert(doc, manipulate=False, w=2)
    end = time.time()
    executionTime = (end - start) * 1000 # Convert to ms
    print('{} -> {} ms'.format(i, executionTime))

def benchmark_parallel(n):
    nprocs = mp.cpu_count()
    pool = mp.Pool(processes=nprocs)
    start = time.time()
    pool.map(excute_query, range(n))
    end = time.time()
    totalTime = (end - start) * 1000
    print('Total = {} ms'.format(totalTime))

def main():
    ap = argparse.ArgumentParser(
        description='MongoDB Benchmarking.'
    )
    ap.add_argument(
        '-m',
        '--mode',
        required=False,
        choices=['serial','parallel'],
        help='serial OR parallel'
    )
    ap.add_argument(
        '-t',
        '--test',
        action='store_true',
        help='Test the connection'
    )
    ap.add_argument(
        '-n',
        '--number',
        required=False,
        type=int,
        help='Number of Requests'
    )
    args = vars(ap.parse_args())
    if args['mode'] == 'serial':
        benchmark(args['number'])
    elif args['mode'] == 'parallel':
        benchmark_parallel(args['number'])
    else:
        test_mongodb()

if __name__ == '__main__':
    main()
