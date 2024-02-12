import logging
import os

from flask import Flask, jsonify, redirect, request, session
from dotenv import load_dotenv
from spotipy import oauth2
import spotipy


load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

auth_manager = oauth2.SpotifyOAuth(scope='user-read-private')


@app.before_request
def check_token():
    if request.endpoint == 'callback':
        return

    match session.get('user_info'):
        case {'token_info': token_info}:
            logger.debug('User is authenticated')
            if auth_manager.is_token_expired(token_info):
                logger.debug('Token is expired, refreshing')
                token_info = auth_manager.refresh_access_token(
                    token_info['refresh_token'])
                session['user_info']['token_info'] = token_info
        case _:
            logger.debug('User is not authenticated')
            return redirect(auth_manager.get_authorize_url())

    return jsonify(session)


@app.route('/')
def index():
    return redirect('/profile')


@app.route('/callback')
def callback():
    code = request.args.get('code')
    token_info = auth_manager.get_access_token(code)
    access_token = token_info['access_token']

    sp = spotipy.Spotify(auth=access_token)
    user = sp.current_user()

    session['user_info'] = {
        'id': user['id'],
        'display_name': user['display_name'],
        'email': user.get('email'),
        'image_url': user['images'][0]['url'] if user['images'] else None,
        'token_info': token_info,
    }

    return redirect('/profile')


@app.route('/profile')
def profile():
    user_info = session['user_info']
    return jsonify(user_info)


if __name__ == '__main__':
    app.run(debug=True)
