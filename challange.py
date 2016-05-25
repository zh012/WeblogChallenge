import sys
from json import dumps
from time import mktime
from functools import partial
from iso8601 import parse_date
from pyspark import SparkConf, SparkContext


SESSION_THRESHOLD = 900
INPUT_DATA_FILE = sys.argv[1]
OUTPUT_SESSIONS_FILE = 'sessions.json'
OUTPUT_AVG_SESSION_FILE = 'average_session_length.json'

conf = SparkConf().setMaster("local").setAppName("Challange")
sc = SparkContext(conf=conf)


def extract_data(line):
    '''
    this funtion is used by the rdd.flatMap to extract
    useful infomation in a robust way
    '''
    try:
        # split a line until the http request
        fields = line.split(' ', 13)
        # return a key/value pair
        return [
            (
                # where the key is ip address
                fields[2].split(':')[0],
                # the value is a dict with request info
                {
                    # timestamp. convert iso8601 date to seconds
                    'ts': mktime(parse_date(fields[0]).timetuple()),
                    # the http request url
                    'url': fields[12]
                }
            )
        ]
    except:
        # return nothing for malformed lines
        return []
        # if interception into malformed lines is needed
        # we can return [('', line)] here, and
        # filter out these later for further processing


def split_sessions(requests, threshold=SESSION_THRESHOLD):
    '''
    this function split sessions for each user, with the given
    session timeout threshold.
    '''
    sessions = []

    # iter over the requests made from one ip, orderd by timestamp
    for req in sorted(requests, key=lambda r: r['ts']):
        ts = req['ts']
        if not sessions or ts - sessions[-1]['end'] >= threshold:
            # add a new session if there is no session yet
            # or the last session is timeout
            sessions.append({'start': ts, 'end': ts, 'hits': 1, 'urls': {req['url']}})
        else:
            # otherwise, update the last session with the
            # current request
            last_session = sessions[-1]
            last_session['end'] = ts
            last_session['hits'] += 1
            last_session['urls'].add(req['url'])
    # get the unique url numbers
    # note: could be done in with rdd, but probably more
    # efficient here if the raw url info is not needed in
    # the future processing
    for s in sessions:
        s['urls'] = len(s['urls'])
    return sessions


def save_to_file(filename, rdd):
    rdd.map(dumps).saveAsTextFile(filename)


#
# sessionize the data
#
sessions = sc.textFile(INPUT_DATA_FILE)\
    .flatMap(extract_data)\
    .groupByKey()\
    .flatMapValues(split_sessions)\
    .cache()


#
# get average session time for each ip
#

# get length of each session
sessions_length = sessions.mapValues(lambda v: v['end'] - v['start']).cache()

# get average session length for each ip
avg_session_time = sessions_length\
    .aggregateByKey(  # aggregate the (sum of session lenght, count of sessions) pair for each key
        # zero values, the first element is the sum value
        # and the second element is the count value
        (0,0),
        # get the sum of session length and count for each partition
        lambda x, y: (x[0] + y, x[1] + 1),
        # combine the results from all partitions
        lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1])
    )\
    .mapValues(lambda x: x[0] / x[1])  # get the average session length for each key


save_to_file(OUTPUT_SESSIONS_FILE, sessions)
save_to_file(OUTPUT_AVG_SESSION_FILE, avg_session_time)


#
# engaged users
#
ips_with_top_session_length = sessions_length\
    .reduceByKey(max)\
    .map(lambda (x,y): (y,x))\
    .sortByKey(ascending=False)

save_to_file(
    'top_100_session_length.json',
    sc.parallelize(ips_with_top_session_length.take(100), 1)
)
