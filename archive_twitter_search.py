#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright 2010 Dave Challis <dsc@ecs.soton.ac.uk>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Edit this to change the twitter search that's performed
query = 'android'

# Edit this to save results to a different SQLite DB
db_path = 'tweets.db'

import sys
import json
import urllib
import urllib2
import dateutil.parser
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.ext.declarative
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime
from collections import defaultdict

# Handles calls to twitter API, paging of search results
class TwitterSearch(object):
    def __init__(self, query):
        self.base_url = 'http://search.twitter.com/search.json'
        self.query = query    # Unescaped query string
        self.rpp = 100        # Results per page
        self.next_page = None # URL params to query next search page
        self.since_id = None  # Search for tweets > this tweet ID

    # Get URL and params to query twitter API with
    def get_search_url(self):
        # Use URL + params returned by twitter API if possible...
        if self.next_page is not None:
            return self.base_url + self.next_page

        # ...otherwise generate our own
        data = {'q': self.query, 'rpp': self.rpp}
        if self.since_id is not None:
            data['since_id'] = self.since_id
        return self.base_url + '?' + urllib.urlencode(data)

    # Call twitter API, return a python dict built from the JSON returned
    def do_query(self):
        request = urllib2.Request(self.get_search_url())
        try:
            response = urllib2.urlopen(request)
        except urllib2.HTTPError, e:
            print 'HTTPError: ', e.code
            sys.exit(1)
        except urllib2.URLError, e:
            print 'URLError: ', e.reason
            sys.exit(1)
        return json.loads(response.read())

    # Returns iterator to loop through each tweet
    def get_tweets(self):
        # Limit number of pages to n_max used as a safety net in case
        # twitter api doesn't stop returning next_page links (twitter should
        # stop at 15). Will need changing if self.rpp is changed.
        n = 0
        n_max = 20

        done = False
        while not done and n < n_max:
            n += 1

            # Query twitter API
            q_results = self.do_query()

            # Return a single tweet each time this function is called
            if q_results.has_key('results'):
                tweets = q_results['results']
                while len(tweets) > 0:
                    yield tweets.pop(0)
            
            # When we run out of tweets, query next page of results if possible
            if q_results.has_key('next_page'):
                self.next_page = q_results['next_page']
            else:
                done = True
            

# Set up sqlite database and ORM for storing tweets
engine = sqlalchemy.create_engine("sqlite:///%s" % db_path)
Session = sqlalchemy.orm.sessionmaker(bind=engine)
Base = sqlalchemy.ext.declarative.declarative_base()

# Basic ORM class to store tweet in DB
class Tweet(Base):
    __tablename__ = 'tweets'

    id = Column(Integer, primary_key=True)
    iso_language_code = Column(String)
    text = Column(String)
    created_at = Column(DateTime(timezone=True))
    profile_image_url = Column(String)
    to_user = Column(String)
    to_user_id = Column(Integer)
    from_user = Column(String)
    from_user_id = Column(Integer)
    source = Column(String)
    geo = Column(String)

    def __init__(self, partial_data):
        # Fill in missing key:value pairs with key:None
        data = defaultdict(lambda:None)
        data.update(partial_data)
        
        self.id = data['id']
        self.iso_language_code = data['iso_language_code']
        self.text = data['text']

        # Convert date to datetime object for storing UTC in DB
        if data['created_at'] is not None:
            self.created_at = dateutil.parser.parse(data['created_at'])
        else:
            self.created_at = None

        self.profile_image_url = data['profile_image_url']
        self.to_user = data['to_user']
        self.to_user_id = data['to_user_id']
        self.from_user = data['from_user']
        self.from_user_id = data['from_user_id']
        self.source = data['from_source']

        # Geo information is of the form:
        # {u'type': u'Point', u'coordinates': [35.073, -77.042900000000003]}]
        # Store easily parsable string form in DB
        if data['geo'] is not None:
            self.geo = repr(data['geo'])
        else:
            self.geo = None
    
# Create DB and tables if needed, create DB connection
Base.metadata.create_all(engine)
session = Session()

# Set up twitter search object
ts = TwitterSearch(query)

# Get ID of latest tweet we have, to avoid searching for tweets we've seen
sql = 'SELECT MAX(id) as i FROM tweets'
row = session.query('i').from_statement(sql).first()

if row[0] is not None:
    ts.since_id = row[0]

# Query search api for as many results as we can get, and add to DB
n_tweets = 0
for tweet in ts.get_tweets():
    tweet_obj = Tweet(tweet)    

    try:
        session.add(tweet_obj)
        session.commit()
        n_tweets += 1
    except sqlalchemy.exc.IntegrityError, e:
        # Search sometimes returns tweets with duplicate ID, rollback and
        # ignore that tweet if this happens
        session.rollback()

print datetime.now().isoformat(), "- %s new tweets added" % n_tweets
