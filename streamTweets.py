import tweepy,json,boto3
from tweepy.streaming import StreamListener
from tokens import consumer_key, consumer_secret, access_token, access_token_secret

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName="tweetsQueue")

keywordList = ["Trump","Hillary","Sanders","Facebook","LinkedIn","Amazon","Google","Uber","Columbia","New York"]

def findCategory(text, keywordList):
    category = []
    for keyword in keywordList:
        if keyword in text:
            category.append(keyword)
    return category

def send_message(body):
    """ sends a message to the AWS queue """
    response = queue.send_message(MessageBody=body)
    print ("INFO: added message " + response.get('MessageId') + "to the queue")

class MyStreamListener(StreamListener):
	
    def __init__(self):
        self.counter = 0
        self.limit = 500
    def on_data(self, data):
        if self.counter < self.limit:
            decoded = json.loads(data)
            if decoded.get('coordinates',None) is not None:
                id = decoded['id']
                time = decoded.get('created_at','')
                text = decoded['text'].lower().encode('ascii','ignore').decode('ascii')
                coordinates = decoded.get('coordinates','').get('coordinates','')
                category = findCategory(text, keywordList)
                tweet = {'timestamp': time,
                        'text': text,
                        'coordinates': coordinates,
                        'category': category,
                        'id': id }
                self.counter += 1
                send_message(json.dumps(tweet))
        else:
            twitterStream.disconnect()

def on_error(self, status):
    print ("error: " + status)

def readkeys():
	keys =[]
	lines = open("keys.txt").readlines()
	for line in lines:
		line_sp = line.split(":")
		keys.append(line_sp[1][:-1])
	return keys

if __name__ == '__main__':

#	keys = readkeys()

	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	myStream = tweepy.Stream(auth = auth, listener=MyStreamListener())
	myStream.filter(track=keywordList, stall_warnings=False)

