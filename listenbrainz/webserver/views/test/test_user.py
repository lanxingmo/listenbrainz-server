import listenbrainz.db.stats as db_stats
import listenbrainz.db.user as db_user
from time import sleep
import json
import ujson
from unittest import mock

from flask import url_for, current_app
from data.model.user_artist_stat import UserArtistStatJson

from listenbrainz.db.testing import DatabaseTestCase
from listenbrainz.listenstore.tests.util import create_test_data_for_timescalelistenstore
from listenbrainz.webserver.timescale_connection import init_timescale_connection
from listenbrainz.webserver.login import User
from listenbrainz.webserver.testing import ServerTestCase

import listenbrainz.db.user as db_user
import logging


class UserViewsTestCase(ServerTestCase, DatabaseTestCase):
    def setUp(self):
        ServerTestCase.setUp(self)
        DatabaseTestCase.setUp(self)

        self.log = logging.getLogger(__name__)
        self.logstore = init_timescale_connection(self.log, {
            'REDIS_HOST': current_app.config['REDIS_HOST'],
            'REDIS_PORT': current_app.config['REDIS_PORT'],
            'REDIS_NAMESPACE': current_app.config['REDIS_NAMESPACE'],
            'SQLALCHEMY_TIMESCALE_URI': self.app.config['SQLALCHEMY_TIMESCALE_URI']
        })

        user = db_user.get_or_create(1, 'iliekcomputers')
        db_user.agree_to_gdpr(user['musicbrainz_id'])
        self.user = User.from_dbrow(user)

        weirduser = db_user.get_or_create(2, 'weird\\user name')
        self.weirduser = User.from_dbrow(weirduser)

    def tearDown(self):
        self.logstore = None
        ServerTestCase.tearDown(self)
        DatabaseTestCase.tearDown(self)

    def test_user_page(self):
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        self.assertContext('active_section', 'listens')

        # check that artist count is not shown if stats haven't been calculated yet
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        self.assertTemplateUsed('user/profile.html')
        props = ujson.loads(self.get_context_variable('props'))
        self.assertIsNone(props['artist_count'])

        with open(self.path_to_data_file('user_top_artists_db.json')) as f:
            artists_data = ujson.load(f)

        db_stats.insert_user_artists(
            user_id=self.user.id,
            artists=UserArtistStatJson(**{'all_time': artists_data})
        )
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        self.assertTemplateUsed('user/profile.html')
        props = ujson.loads(self.get_context_variable('props'))
        self.assertEqual(props['artist_count'], '2')
        self.assertDictEqual(props['spotify'], {})

    @mock.patch('listenbrainz.webserver.views.user.spotify')
    def test_spotify_token_access(self, mock_domain_spotify):
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        self.assertTemplateUsed('user/profile.html')
        props = ujson.loads(self.get_context_variable('props'))
        self.assertDictEqual(props['spotify'], {})

        self.temporary_login(self.user.login_id)
        mock_domain_spotify.get_user_dict.return_value = {}
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        props = ujson.loads(self.get_context_variable('props'))
        self.assertDictEqual(props['spotify'], {})

        mock_domain_spotify.get_user_dict.return_value = {
            'access_token': 'token',
            'permission': 'permission',
        }
        response = self.client.get(url_for('user.profile', user_name=self.user.musicbrainz_id))
        self.assert200(response)
        props = ujson.loads(self.get_context_variable('props'))
        mock_domain_spotify.get_user_dict.assert_called_with(self.user.id)
        self.assertDictEqual(props['spotify'], {
            'access_token': 'token',
            'permission': 'permission',
        })

        response = self.client.get(url_for('user.profile', user_name=self.weirduser.musicbrainz_id))
        self.assert200(response)
        props = ujson.loads(self.get_context_variable('props'))
        mock_domain_spotify.get_user_dict.assert_called_with(self.user.id)
        self.assertDictEqual(props['spotify'], {
            'access_token': 'token',
            'permission': 'permission',
        })

    def _create_test_data(self, user_name):
        min_ts = -1
        max_ts = -1
        self.test_data = create_test_data_for_timescalelistenstore(user_name)
        for listen in self.test_data:
            if min_ts < 0 or listen.ts_since_epoch < min_ts:
                min_ts = listen.ts_since_epoch
            if max_ts < 0 or listen.ts_since_epoch > max_ts:
                max_ts = listen.ts_since_epoch

        self.logstore.insert(self.test_data)
        return (min_ts, max_ts)

    def test_username_case(self):
        """Tests that the username in URL is case insensitive"""
        self._create_test_data('iliekcomputers')

        response1 = self.client.get(url_for('user.profile', user_name='iliekcomputers'))
        self.assertContext('user', self.user)
        response2 = self.client.get(url_for('user.profile', user_name='IlieKcomPUteRs'))
        self.assertContext('user', self.user)
        self.assert200(response1)
        self.assert200(response2)

    @mock.patch('listenbrainz.webserver.timescale_connection._ts.get_timestamps_for_user')
    @mock.patch('listenbrainz.webserver.timescale_connection._ts.fetch_listens')
    def test_ts_filters(self, timescale, timestamps):
        """Check that max_ts and min_ts are passed to timescale """
        (min_ts, max_ts) = self._create_test_data('iliekcomputers')
        timestamps.return_value = (min_ts, max_ts)

        # If no parameter is given, use current time as the to_ts
        self.client.get(url_for('user.profile', user_name='iliekcomputers'))
        req_call = mock.call('iliekcomputers', limit=25, to_ts=1400000201, time_range=None)
        timescale.assert_has_calls([req_call])
        timescale.reset_mock()

        # max_ts query param -> to_ts timescale param
        self.client.get(url_for('user.profile', user_name='iliekcomputers'), query_string={'max_ts': 1520946000})
        req_call = mock.call('iliekcomputers', limit=25, to_ts=1520946000, time_range=None)
        timescale.assert_has_calls([req_call])
        timescale.reset_mock()

        # min_ts query param -> from_ts timescale param
        self.client.get(url_for('user.profile', user_name='iliekcomputers'), query_string={'min_ts': 1520941000})
        req_call = mock.call('iliekcomputers', limit=25, from_ts=1520941000, time_range=None)
        timescale.assert_has_calls([req_call])
        timescale.reset_mock()

        # If max_ts and min_ts set, only max_ts is used
        self.client.get(url_for('user.profile', user_name='iliekcomputers'),
                        query_string={'min_ts': 1520941000, 'max_ts': 1520946000})
        req_call = mock.call('iliekcomputers', limit=25, to_ts=1520946000, time_range=None)
        timescale.assert_has_calls([req_call])

    @mock.patch('listenbrainz.webserver.timescale_connection._ts.get_timestamps_for_user')
    @mock.patch('listenbrainz.webserver.timescale_connection._ts.fetch_listens')
    def test_ts_filters_errors(self, timescale, timestamps):
        """If max_ts and min_ts are not integers, show an error page"""
        (min_ts, max_ts) = self._create_test_data('iliekcomputers')
        timestamps.return_value = (min_ts, max_ts)

        response = self.client.get(url_for('user.profile', user_name='iliekcomputers'),
                                   query_string={'max_ts': 'a'})
        self.assert400(response)
        self.assertIn(b'Incorrect timestamp argument max_ts: a', response.data)

        response = self.client.get(url_for('user.profile', user_name='iliekcomputers'),
                                   query_string={'min_ts': 'b'})
        self.assert400(response)
        self.assertIn(b'Incorrect timestamp argument min_ts: b', response.data)

        timescale.assert_not_called()

    @mock.patch('listenbrainz.webserver.timescale_connection._ts.get_timestamps_for_user')
    @mock.patch('listenbrainz.webserver.timescale_connection._ts.fetch_listens')
    def test_search_larger_time_range_filter(self, timescale, timestamps):
        """Check that search_larger_time_range is passed to timescale """
        (min_ts, max_ts) = self._create_test_data('iliekcomputers')
        timestamps.return_value = (min_ts, max_ts)

        # If search_larger_time_range is not given, use search_larger_time_range=0
        self.client.get(url_for('user.profile', user_name='iliekcomputers'))
        req_call = mock.call('iliekcomputers', limit=25, to_ts=1400000201, time_range=None)
        timescale.assert_has_calls([req_call])
        timescale.reset_mock()

        # search_larger_time_range query param -> search_larger_time_range timescale param
        self.client.get(url_for('user.profile', user_name='iliekcomputers'), query_string={'search_larger_time_range': 1})
        req_call = mock.call('iliekcomputers', limit=25, to_ts=1400000201, time_range=10)
        timescale.assert_has_calls([req_call])
        timescale.reset_mock()

    @mock.patch('listenbrainz.webserver.timescale_connection._ts.get_timestamps_for_user')
    @mock.patch('listenbrainz.webserver.timescale_connection._ts.fetch_listens')
    def test_search_larger_time_range_filter_errors(self, timescale, timestamps):
        """If search_larger_time_range is not integer, show an error page"""
        (min_ts, max_ts) = self._create_test_data('iliekcomputers')
        timestamps.return_value = (min_ts, max_ts)
        response = self.client.get(url_for('user.profile', user_name='iliekcomputers'),
                                   query_string={'search_larger_time_range': 'a'})
        self.assert400(response)
        self.assertIn(b'search_larger_time_range must be an integer value 0 or greater: a', response.data)

        timescale.assert_not_called()


    def test_delete_listen(self):
        self.temporary_login(self.user.login_id)
        self._create_test_data(self.user.musicbrainz_id)
        delete_listen_url = url_for('user.delete_listen', user_name=self.user.musicbrainz_id)

        listens = self.logstore.fetch_listens(user_name=self.user.musicbrainz_id, from_ts=1399999999)
        self.assertEqual(len(listens), 5)

        delete_listen = self.test_data[0]
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'listened_at': delete_listen.ts_since_epoch,
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assert200(response)
        self.assertEqual(response.json["status"], "ok")

        listens = self.logstore.fetch_listens(user_name=self.user.musicbrainz_id, from_ts=1399999999)
        self.assertEqual(len(listens), 4)

        self.assertNotIn(delete_listen, listens)

    def test_delete_listen_not_logged_in(self):
        self._create_test_data(self.user.musicbrainz_id)
        delete_listen_url = url_for('user.delete_listen', user_name=self.user.musicbrainz_id)

        listens = self.logstore.fetch_listens(user_name=self.user.musicbrainz_id, from_ts=1399999999)
        self.assertEqual(len(listens), 5)

        delete_listen = self.test_data[0]
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'listened_at': delete_listen.ts_since_epoch,
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assertStatus(response, 302)
        self.assertRedirects(response, url_for('login.index', next=delete_listen_url))

    def test_delete_listen_missing_keys(self):
        self.temporary_login(self.user.login_id)
        self._create_test_data(self.user.musicbrainz_id)
        delete_listen_url = url_for('user.delete_listen', user_name=self.user.musicbrainz_id)

        listens = self.logstore.fetch_listens(user_name=self.user.musicbrainz_id, from_ts=1399999999)
        self.assertEqual(len(listens), 5)

        delete_listen = self.test_data[0]

        # send request without auth_token
        response = self.client.post(delete_listen_url,
                                    data={
                                        'listened_at': delete_listen.ts_since_epoch,
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assertStatus(response, 401)

        # send request without listened_at
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assertStatus(response, 400)
        self.assertEqual(response.json["error"], "Listen timestamp missing.")

        # send request without recording_msid
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'listened_at': delete_listen.ts_since_epoch
                                        })
        self.assertStatus(response, 400)
        self.assertEqual(response.json["error"], "Recording MSID missing.")

    def test_delete_listen_invalid_keys(self):
        self.temporary_login(self.user.login_id)
        self._create_test_data(self.user.musicbrainz_id)
        delete_listen_url = url_for('user.delete_listen', user_name=self.user.musicbrainz_id)

        listens = self.logstore.fetch_listens(user_name=self.user.musicbrainz_id, from_ts=1399999999)
        self.assertEqual(len(listens), 5)

        delete_listen = self.test_data[0]

        # send request with invalid auth_token
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': 'invalid token',
                                        'listened_at': delete_listen.ts_since_epoch,
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assertStatus(response, 401)

        # send request with invalid listened_at
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'listened_at': 'invalid listened_at',
                                        'recording_msid': delete_listen.recording_msid
                                        })
        self.assertStatus(response, 400)
        self.assertEqual(response.json["error"], "invalid listened_at: Listen timestamp invalid.")

        # send request with invalid recording_msid
        response = self.client.post(delete_listen_url,
                                    data={
                                        'token': self.user.auth_token,
                                        'listened_at': delete_listen.ts_since_epoch,
                                        'recording_msid': 'invalid recording_msid'
                                        })
        self.assertStatus(response, 400)
        self.assertEqual(response.json["error"], "invalid recording_msid: Recording MSID format invalid.")
