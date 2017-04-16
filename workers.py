import boto3
import gevent
import json
import random
from elasticsearch import Elasticsearch, exceptions, RequestsHttpConnection
import requests
from requests_aws4auth import AWS4Auth
from textblob import TextBlob

WORKERS = 10
API_URL = "http://gateway-a.watsonplatform.net/calls/text/TextGetTextSentiment"
API_TOKEN = "0ac5fb44df7c0b67834d33197cb4117472020536"
QUEUE_NAME = "tweetsQueue"
WAIT_TIME = 10 # time to wait between each SQS poll
TOPIC_NAME = "tweet-topic"
SNS_ARN = "arn:aws:sns:us-west-2:323741440251:tweet-topic"
aws_access_key_id="AKIAJKM5RPMKX6TCA5HA"
aws_secret_access_key="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"
REGION = "us-west-2"

awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, REGION, 'es')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
sns = boto3.client('sns')

es = Elasticsearch(
                   hosts=[{'host': 'search-twittmap2-y3vhxznnzil3c4vi6wqce7x3n4.us-west-2.es.amazonaws.com', 'port': 443}],
                   use_ssl=True,
                   http_auth=awsauth,
                   verify_certs=True,
                   connection_class=RequestsHttpConnection
                   )

def task(pid):
    print ("[Task " + str(pid) + "] Starting ...")
    while True:
        for message in queue.receive_messages():
            tweet = json.loads(message.body)
            re = TextBlob(tweet['text'])
            resp = re.sentiment.polarity
            if resp < 0:
                sentiment = "negative"
            elif resp == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"
 #           payload = {
 #               'apikey': API_TOKEN, "outputMode": "json", "text": tweet["text"]
 #           }
 #           r = requests.get(API_URL, params=payload)
 #           if r.status_code == 200 and r.json().get("status") != "ERROR":
            if sentiment != "":
                tweet["sentiment"] = sentiment
                print("Sentiment: "+ str(tweet['sentiment']))
                # index tweet in ES
                res = es.index(index="tweets", doc_type="tweet", id=tweet["id"], body=tweet)
                
                # send notification
                sns.publish(
                            TopicArn=SNS_ARN,
                            Message=json.dumps(tweet),
                            Subject='New Tweet')
                            
                print ("[Task " + str(pid) + ", tweetid " + str(tweet["id"]) + " indexed")
            message.delete()
        gevent.sleep(WAIT_TIME)


if __name__ == "__main__":
    threads = [gevent.spawn(task, pid) for pid in range(1, WORKERS+1)]
    gevent.joinall(threads)