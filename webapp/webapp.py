from flask import Flask, render_template, request
import requests
from flask_sockets import Sockets
import gevent
import redis
import json
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, exceptions, RequestsHttpConnection

aws_access_key_id="AKIAJKM5RPMKX6TCA5HA"
aws_secret_access_key="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"
REGION = "us-west-2"

awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, REGION, 'es')

app = Flask(__name__)
#app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
sockets = Sockets(app)
es = Elasticsearch(
    hosts=[{'host': 'search-twittmap2-y3vhxznnzil3c4vi6wqce7x3n4.us-west-2.es.amazonaws.com', 'port': 443}],
    use_ssl=True,
    http_auth=awsauth,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

REDIS_URL = "http://localhost:6379"
REDIS_CHAN = "tweet"
redis = redis.from_url(REDIS_URL)

keywords = ["Trump","Hillary","Sanders","Facebook","LinkedIn","Amazon","Google","Uber","Columbia","New York"]

def msg_process(msg, tstamp):
    message = json.loads(msg)
    redis.publish(REDIS_CHAN, message)
    print ("published msg to redis at", tstamp)

class WebsocketTracker(object):
    def __init__(self):
        self.clients = list()
        self.pubsub = redis.pubsub()
        self.pubsub.subscribe(REDIS_CHAN)

    def __iter_data(self):
        for message in self.pubsub.listen():
            data = message.get('data')
            if message['type'] == 'message':
                app.logger.info(u'Sending message: {}'.format(data))
                yield data

    def register(self, client):
        self.clients.append(client)
        print ("registering a new client. total clients:", len(self.clients))
        msg = { "msg": "connected", "type": "INFO" }
        client.send(json.dumps(msg))

    def send(self, client, data):
        try:
            msg = { "msg": "got a new msg", "type": "UPDATE" }
            client.send(json.dumps(msg))
            print ("data sent to client")
        except Exception:
            print ("some issue. removing client")
            self.clients.remove(client)
            print ("removed a client. total clients:" + len(self.clients))

    def run(self):
        for data in self.__iter_data():
            for client in self.clients:
                gevent.spawn(self.send, client, data)

    def start(self):
        print (u'Started listening for messages')
        gevent.spawn(self.run)

tracker = WebsocketTracker()
tracker.start()

@app.route('/')
def index():
    query = request.args.get('q', '')
    res = es.search(
        index="tweets",
        body={
            "query": {"match": {"text": query}},
            "size": 750 # max document size
        })
    coords, results = [], []
    if res["hits"]["hits"]:
        coords = [r["_source"]["coordinates"] for r in res["hits"]["hits"]]
        results = [r["_source"]["text"] for r in res["hits"]["hits"]]
    return render_template("index.html",
                           coords=coords,
                           keywords=keywords,
                           query=query,
                           results=results)

@app.route('/notify', methods=['GET', 'POST', 'PUT'])
def handle_notify():
    js = json.loads(request.data)
    hdr = request.headers.get('X-Amz-Sns-Message-Type')
    if hdr == 'SubscriptionConfirmation' and 'SubscribeURL' in js:
        r = requests.get(js['SubscribeURL'])

    if hdr == 'Notification':
        msg_process(js['Message'], js['Timestamp'])
    return 'OK\n'

@sockets.route('/receive')
def outbox(ws):
    tracker.register(ws)
    while not ws.closed:
        gevent.sleep(0.1)

if __name__ == "__main__":
    app.run(debug=True, port=5000, host="0.0.0.0")
