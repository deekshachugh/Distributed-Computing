from pymongo import MongoClient
from random import randint
from random import choice
from datetime import datetime

def get_random_message():
    msg = [
    "Heya",
    "you look great",
    "can we meet",
    "meet me",
    "I like you",
    "call me back",
    "Hey",
    "you are so beautiful",
    "you are so mean",
    "i hate you",
    "I love you"
    ]
    return msg[randint(0, len(msg) - 1)]

def create_random_message():
    r_id = ''.join(choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(12))
    s_id = ''.join(choice('0123456789abcdefghijklmnopqrstuvwxyz') for i in range(12))
    msg = get_random_message()
    ts = datetime.utcnow().strftime("%s")
    return {'r_id':r_id,'s_id':s_id,'msg':msg,'ts':ts}

client = MongoClient('localhost', 27017)

db = client.dating_site

#for i in range(100000):
#    m_id = db.messages.insert(create_random_message())


def find_keyword(keywords):
    res = db.messages.find({"msg": {'$regex': '.*'+keywords+'.*'} })
    for collection in res:
       print "Sender ID: %s, Receiver ID: %s" % (collection["s_id"], collection["r_id"])#client = MongoClient('localhost', 27017)


def main():
    keywords="like"
    find_keyword(keywords)


main()