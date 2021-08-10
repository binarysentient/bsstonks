"""
Zerodha has the following rate limits on the API:

Quote API rate limit: 1/second
Order placement API rate limit: 5 or 10/second restricted to 200 order placement per minute
Historical API rate limit: 3 per second
WebSocket API limit: Subscription to 3,000 instruments.
Order modification: 25 modifications per order.
"""

import os
import kiteconnect
# import logging
import requests
import json
import asyncio
from threading import Thread
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse
from urllib.parse import parse_qs
import time

from pyppeteer import launch
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
KITE_CONNECT_API_KEY = os.getenv("KITE_CONNECT_API_KEY")
KITE_CONNECT_API_SECRET = os.getenv("KITE_CONNECT_API_SECRET")
BSSTONKS_DIRECTORY = os.getenv("BSSTONKS_DIRECTORY")

# logging.basicConfig(level=logging.DEBUG)

def _save_kite_access_token(api_key, the_token):
    savelocpath = ".kite_access_token.json"
    if BSSTONKS_DIRECTORY is not None:
        savelocpath = os.path.join(BSSTONKS_DIRECTORY, savelocpath)
    with open(savelocpath,'w') as tokenfile:
        json.dump({f'{api_key}':the_token},tokenfile)

def _read_kite_access_token(validate_token=True):
    tokenmap = None
    savelocpath = ".kite_access_token.json"
    if BSSTONKS_DIRECTORY is not None:
        savelocpath = os.path.join(BSSTONKS_DIRECTORY, savelocpath)
    if not os.path.exists(savelocpath):
        return None
    with open(savelocpath,'r') as tokenfile:
        tokenmap = json.load(tokenfile)
    if validate_token:
        if tokenmap is not None:
            kite = KiteConnect(api_key=KITE_CONNECT_API_KEY)
            kite.set_access_token(tokenmap[KITE_CONNECT_API_KEY])
            try:
                kite.profile()
                #token is valid if this suceeds
            except kiteconnect.exceptions.TokenException as tokenexception:
                return None
    return tokenmap

def _fetch_access_token_from_request_token(request_token):
    kite = KiteConnect(api_key=KITE_CONNECT_API_KEY)
    
    if type(request_token) == list:
        request_token = request_token[0]
    
    kite_session_data = kite.generate_session(request_token, api_secret=KITE_CONNECT_API_SECRET)
    
    access_token = kite_session_data["access_token"]
    return access_token

def _kiteconnect_login_and_fetch_request_token():
    kite = KiteConnect(api_key=KITE_CONNECT_API_KEY)
    # webbrowser.open(kite.login_url(), new=2)
    # print(kite.login_url())
    # return
    kite_username = os.getenv("KITE_USERNAME")
    kite_password = os.getenv("KITE_PASSWORD")
    kite_pin = os.getenv("KITE_PIN")
    request_token_url = None
    async def _kite_login_auto(theurl):
        nonlocal request_token_url
        nonlocal kite_username
        nonlocal kite_password
        nonlocal kite_pin
        login_button_selector = ".actions button"
        username_input_selector = "input#userid"
        password_input_selector = "input#password"
        pin_input_selector = "input#pin"
        pin_continue_button_selector = ".actions button"

        browser = await launch({'headless': False, 'handleSIGINT':False, 'handleSIGTERM':False, 'handleSIGHUP':False})
        page = await browser.newPage()
        page.setDefaultNavigationTimeout(3*1000)
        # go to the login url
        await page.goto(theurl)
        # wait for login button to appear
        await page.waitForSelector(login_button_selector)
        # enter username password and click login
        await page.type(username_input_selector, kite_username, options={"delay":30})
        await page.type(password_input_selector, kite_password, options={"delay":30})
        await page.click(login_button_selector)
        # wait for the pin page to appear
        await page.waitForSelector(pin_input_selector)
        # enter pin and click continue
        await page.type(pin_input_selector, kite_pin, options={"delay":30})
        await page.click(pin_continue_button_selector)
        # this is our listener/server specific tag we wait for
        await page.waitForSelector("p#bs_requesttoken_accepted")
        request_token_url = page.url
        await browser.close()

    asyncio.get_event_loop().run_until_complete(_kite_login_auto(kite.login_url()))
    return request_token_url





class KiteTokenReceiverHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        if 'request_token' in self.path:
            self.wfile.write(bytes("<html><head><title>BSStonks</title></head>", "utf-8"))
            
            parsed = urlparse.urlparse(self.path)
            request_token = parse_qs(parsed.query)['request_token']
            kite_access_token = _fetch_access_token_from_request_token(request_token)
            _save_kite_access_token(KITE_CONNECT_API_KEY, kite_access_token)

            self.wfile.write(bytes("<p id='bs_requesttoken_accepted'> Request Token is: %s</p>" % request_token, "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>You can close this page now!</p>", "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))

class KiteTokenReceiverServerThread(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.daemon=True
        
    def run(self):
        hostName = "0.0.0.0"
        serverPort = int(os.getenv("KITE_CONNECT_RECEIVE_TOKEN_PORT",default="8585"))
        webServer = HTTPServer((hostName, serverPort), KiteTokenReceiverHandler)
        # print("Server started http://%s:%s" % (hostName, serverPort))

        try:
            webServer.serve_forever()
        # except KeyboardInterrupt:
        #     pass
        finally:
            webServer.server_close()
            print("Server stopped.")





def make_kiteconnect_api() -> KiteConnect:
    tokenmap = _read_kite_access_token()
    kite = KiteConnect(api_key=KITE_CONNECT_API_KEY)
    if tokenmap is None:
        request_token_url = _kiteconnect_login_and_fetch_request_token()
        # parsed = urlparse.urlparse(request_token_url)
        # request_token = parse_qs(parsed.query)['request_token']
        # kite_access_token = _fetch_access_token_from_request_token(request_token)
        # _save_kite_access_token(KITE_CONNECT_API_KEY, kite_access_token)
    tokenmap = _read_kite_access_token()
    
    kite.set_access_token(tokenmap[KITE_CONNECT_API_KEY])
    return kite

_server_thread = KiteTokenReceiverServerThread()
_server_thread.daemon = True
_server_thread.start()

if __name__ == "__main__":

    # kite = KiteConnect(api_key=KITE_CONNECT_API_KEY)
    # print(kite.login_url())
    kite = get_kite_session()
    
    while True:
        time.sleep(1)

    
    
