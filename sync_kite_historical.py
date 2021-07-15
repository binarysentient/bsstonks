from libstonks.kite_historical import get_instrument_list, sync_instrument_list
from utils.bs_threading import bs_multiprocessify

if __name__ == "__main__":
    # sync_instrument_list()
    print(get_instrument_list())