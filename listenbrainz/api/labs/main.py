#!/usr/bin/env python3

from datasethoster.main import app, register_query
from listenbrainz.api.labs.api.artist_country_from_artist_mbid import ArtistCountryFromArtistMBIDQuery
from listenbrainz.api.labs.api.artist_credit_from_artist_mbid_query import ArtistCreditIdFromArtistMBIDQuery
from listenbrainz.api.labs.api.artist_credit_from_artist_msid import ArtistCreditFromArtistMSIDQuery
from listenbrainz.webserver import load_config

register_query(ArtistCountryFromArtistMBIDQuery())
register_query(ArtistCreditIdFromArtistMBIDQuery())
register_query(ArtistCreditFromArtistMSIDQuery())

load_config(app)
