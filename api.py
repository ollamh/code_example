import logging
import json
import urllib2
import time
from datetime import date, datetime, timedelta


def prepare_date(date):
    return str(int(time.mktime(datetime.strptime(date, '%Y%m%d').timetuple())))

class JawboneProviderError(BaseException):
    pass
    
class EndpointError(BaseException):
    pass


class OldJawboneProvider(object):
    """
    XXX: This is an old implementation of the provider.
    Class for getting token from user and managing data extraction from Jawbone API gateway
    Usage:
       First-time:
           Initialize this Provider with no params
           Redirect user to get_auth_code_url to get the confirmation for your app access to user's data
           Receive code and re-initialize provider on redirect_uri endpoint (e.g. http://localhost/code_receiver) 
               (should be already registered on Jawbone website)
           Get token with get_token method
           Save token somewhere
           Profit! You can use this instance to get user data
       After getting token:
           Initialize provider class with token string
           Use it.
    """
    # XXX: HTTPS redirect uri is a requirement for authentication
    # when an app is published.
    BASE_SITE = 'https://jawbone.com'

    AUTH_URL = BASE_SITE + '/auth/oauth2/auth'
    TOKEN_URL = BASE_SITE + '/auth/oauth2/token'

    # Bit application bound for localhost
    CLIENT_ID = '<CLIENT ID>'
    CLIENT_SECRET = '<CLIENT SECRET>'

    SCOPES = {
        'read': [
            'basic_read', 'extended_read', 'location_read',
            'friends_read', 'mood_read', 'move_read', 'sleep_read',
            'meal_read', 'weight_read', 'cardiac_read',
            'generic_event_read'
        ],
        'write': [
            'basic_read', 'extended_read', 'location_read',
            'friends_read', 'mood_read', 'move_read', 'sleep_read',
            'meal_read', 'weight_read', 'cardiac_read',
            'generic_event_read', 'mood_write', 'move_write', 'meal_write', 'weight_write',
            'cardiac_write', 'generic_event_write'
        ]
    }

    READ_SCOPE = ' '.join(SCOPES['read'])
    RW_SCOPE = ' '.join(SCOPES['write'])

    HEADERS = {'Accept': 'application/json'}

    ENDPOINTS = {
        # Users
        'me': '/nudge/api/v.1.0/users/@me',
        'friends': '/nudge/api/v.1.0/users/@me/friends',
        # Mood
        'mood_list': '/nudge/api/v.1.0/users/@me/mood',
        'mood': '/nudge/api/v.1.0/mood/{0}',
        # Trends
        'trends': '/nudge/api/v.1.0/users/@me/trends',
        # Moves
        'moves_date': '/nudge/api/v.1.0/users/@me/moves?date={0}',
        'moves_list': '/nudge/api/v.1.0/users/@me/moves?start_time={0}&end_time={1}',
        'move': '/nudge/api/v.1.0/moves/{0}',
        'move_image': '/nudge/api/v.1.0/moves/{0}/image',
        'move_snapshot': '/nudge/api/v.1.0/moves/{0}/snapshot',
        # Workouts
        'workouts_list': '/nudge/api/v.1.0/users/@me/workouts?start_time={0}&end_time={1}',
        'workout': '/nudge/api/v.1.0/workouts/{0}',
        'workout_image': '/nudge/api/v.1.0/workouts/{0}/image',
        'workout_snapshot': '/nudge/api/v.1.0/workouts/{0}/snapshot',
        # Sleeps
        'sleep_list': '/nudge/api/v.1.0/users/@me/sleeps?start_time={0}&end_time={1}',
        'sleep': '/nudge/api/v.1.0/sleeps/{0}',
        'sleep_image': '/nudge/api/v.1.0/sleeps/{0}/image',
        'sleep_snapshot': '/nudge/api/v.1.0/sleeps/{0}/snapshot',
        # Meals
        'meals_list': '/nudge/api/v.1.0/users/@me/meals?start_time={0}&end_time={1}',
        'meal': '/nudge/api/v.1.0/meals/{0}',
        # Body events
        'body_events_list': '/nudge/api/v.1.0/users/@me/body_events?start_time={0}&end_time={1}',
        'body_event': '/nudge/api/v.1.0/body_events/{0}',
        # Cardiac events
        'cardiac_events_list': '/nudge/api/v.1.0/users/@me/cardiac_events?start_time={0}&end_time={1}',
        'cardiac_event': '/nudge/api/v.1.0/cardiac_events/{0}',

        # XXX: there are more REST urls, but in this implementation
        # we don't need them
    }

    def __init__(self, code=None, token=None, token_type='Bearer'):
        self.logger = logging.getLogger(__name__)
        self.code = code
        self.token = token
        self.token_type = token_type
        self.headers = self.HEADERS
        # We have got token already
        if self.token:
            self.headers['Authorization'] = '{0} {1}'.format(self.token_type, self.token)
        self.opener = urllib2.build_opener()

    def make_request(self, url, body='', method='GET'):
        #print url
        request = urllib2.Request(url, body, self.headers)
        request.get_method = lambda: method
        f = self.opener.open(request)
        resp = f.read()
        if f.info().getheader('Content-Type') == 'application/json':
            resp = json.loads(resp)
        return resp

    def get_auth_code_url(self, redirect_uri, scope='read'):
        if self.code:
            raise Exception('You have the code: {0}'.format(self.code))
        scope = self.READ_SCOPE if scope == 'read' else self.RW_SCOPE
        url = '{0}?response_type=code&client_id={1}&scope={2}&redirect_uri={3}'.format(
            self.AUTH_URL, self.CLIENT_ID, scope, redirect_uri)
        return url

    def get_token(self):
        url = '{0}?grant_type=authorization_code&client_id={1}&client_secret={2}&code={3}'.format(
            self.TOKEN_URL, self.CLIENT_ID, self.CLIENT_SECRET, self.code)

        response = self.make_request(url, '')
        self.headers['Authorization'] = '{0} {1}'.format(response['token_type'], response['access_token'])
        self.token = response['access_token']
        self.token_type = response['token_type']
        return response

    def _raw_get(self, url):
        response = self.make_request(url)
        return response['data']

    def _get(self, what='me', *args, **kwargs):
        """
        Protected method for getting data:
        @params: list of variables needed to complete command url, e.g.
        for 'move' command url '/nudge/api/v.1.0/moves/{0}'
        we have to provide argument 'move_id', etc.
        """
        try:
            response = self.make_request('{0}{1}'.format(self.BASE_SITE, self.ENDPOINTS[what].format(*args)))
            return response['data']
        except KeyError:
            raise EndpointError('No such endpoint')

    def __getattr__(self, attribute):
        """
        Here some magic happens. It's for translation calls like:
        provider.move(args) into
        provider._get('move', args)
        """
        if attribute in self.ENDPOINTS:
            def wrapper(*args, **kwargs):
                return getattr(self, '_get')(attribute, *args, **kwargs)
            return wrapper
        raise AttributeError(attribute)

    def profile(self):
        return self.me()

    # XXX: I know, it's kinda ugly, but the other way is more ugly, believe me
    # Set of list methods with common params: date in 'YYYYMMDD' format
    def moves(self, start_date, finish_date):
        return self.moves_list(prepare_date(start_date), prepare_date(finish_date))

    def sleeps(self, start_date, finish_date):
        return self.sleep_list(prepare_date(start_date), prepare_date(finish_date))

    def workouts(self, start_date, finish_date):
        return self.workouts_list(prepare_date(start_date), prepare_date(finish_date))

    def meals(self, start_date, finish_date):
        return self.meals_list(prepare_date(start_date), prepare_date(finish_date))

    def body_events(self, start_date, finish_date):
        return self.body_events_list(prepare_date(start_date), prepare_date(finish_date))

    def cardiac_events(self, start_date, finish_date):
        return self.cardiac_events_list(prepare_date(start_date), prepare_date(finish_date))

    def total(self, start_date, finish_date):
        """
        Returns total statistics for certain period
        @params: start and finish date in 'YYYYMMDD' format
        """
        assert start_date <= finish_date  # XXX: Yes, i've tested that at least in python 2.7
        start = datetime.strptime(start_date, '%Y%m%d')
        finish = datetime.strptime(finish_date, '%Y%m%d')
        results = {int((start + timedelta(d)).strftime('%Y%m%d')): dict() for d in xrange((finish-start).days+1)}
        for activity in ('moves', 'sleeps', 'workouts', 'meals', 'body_events', 'cardiac_events'):
            res = getattr(self, activity) (start_date, finish_date)
            for item in res['items']:
                results[item['date']][activity[:-1]] = getattr(self, activity[:-1])(item['xid'])
                try:
                    results[item['date']]['{0}_snapshot'.format(activity[:-1])] =\
                        getattr(self, '{0}_snapshot'.format(activity[:-1]))(item['xid'])
                except EndpointError:
                    pass
        return results

    def get_user_register_date(self, value=None, low=date(2011, 1, 1), high=date.today()):
        """
        Uses binary search to find out when user has joined Jawbone
        Usage:
            get_user_register_date()
        Returns:
            datetime.date instance or None if no data were provided
            since Jan 2011
        """
        self.logger.info('Registration date search in period of %s - %s' % (low, high))
        if high - low <= timedelta(days=1):
            return value

        # TODO: check, it shouldn't ever work
        mid = low + (high-low) / 2
        resp = self.moves_date(mid.strftime('%Y%m%d'))

        if resp.get('items'):
            value = mid
            # We have found value with data, so move left
            value = self.get_user_register_date(value, low=low, high=value)
        else:
            # We haven't found value, continue dividing
            value = self.get_user_register_date(value, low=mid, high=high)

        return value
