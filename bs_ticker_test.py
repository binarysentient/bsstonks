import time
from libstonks.bs_kiteconnect import KITE_CONNECT_API_KEY, make_kiteconnect_api, _read_kite_access_token
from libstonks.bs_kiteticker import BSKiteTicker


make_kiteconnect_api()
kiteconnect_access_tokenmap = _read_kite_access_token()
KITE_ACCESS_TOKEN = kiteconnect_access_tokenmap[KITE_CONNECT_API_KEY]

bs_ticker = BSKiteTicker(KITE_CONNECT_API_KEY, KITE_ACCESS_TOKEN)
bs_ticker.start()
sub = 0
while True:
    print("Enter tokens:")
    tokens = input()
    tokens = tokens.split(",")
    tokens = [int(token) for token in tokens]
    print("1. sub \n 2.unnsub \n 3.setmode")
    mode = input()
    if mode == "1":
        bs_ticker.subscribe(tokens)
    if mode == "2":
        bs_ticker.unsubscribe(tokens)
    if mode == "3":
        themode = input()
        if themode in ["ltp","quote","full"]:
            bs_ticker.set_mode(tokens, themode)

    # time.sleep(5)
    if sub == 0:
        sub += 1
        # bs_ticker.subscribe([738561])
        continue
    if sub == 1:
        sub += 1
        # bs_ticker.subscribe([5633])
        continue