import requests
import json
import random
import re
from concurrent.futures import ThreadPoolExecutor
import os


def is_valid_btc_address(addr):
    """
    Checks if the given Bitcoin address is of a valid format.
    """
    return re.fullmatch(r"(bc1|[13])[a-zA-Z0-9]{25,61}", addr)


def blockstream_fetch(addr, output_path):
    """
    Makes a call to the Blockstream API with the given Bitcoin address, 
    and if the request is successful, the response is written to the given output path.
    """
    assert is_valid_btc_address(addr)
    
    url = f"https://blockstream.info/api/address/{addr}"
    try:
        res = requests.get(url, timeout=10)
        print(addr, res.status_code)
    
        if res.status_code == 200:
            with open(output_path, 'a') as f:
                json.dump(res.json(), f)
                f.write('\n')
    except Exception as e:
        print(e)
    

def blockstream_fetch_all(addr_path, 
                          output_path="blockstream.txt", 
                          nr_of_threads=20
                          ):
    """
    Iterates through a list of Bitcoin addresses, calls the Blockstream API for each, 
    and saves successful requests to a file. This function utilizes multithreading.
    """
    executor = ThreadPoolExecutor(max_workers=nr_of_threads)
    
    with open(addr_path) as f:  
        btc_addrs = [addr.strip() for addr in f.readlines()]
    executor.map(blockstream_fetch, btc_addrs, [output_path] * len(btc_addrs))
    executor.shutdown()
    print("All calls done.")
                  

# ----- Note -----
# Below is Bitcoin data retrieval code for the Blockcypher and Blockchain APIs,
# which we did not end up using for the paper.
# ----------------

PROXIES_PATH = "proxies.txt"
ACTIVE_PROXIES_PATH = "active-proxies.txt"  


def proxy_is_active(proxy, save_active_proxies=False):
    """
    Checks if the given proxy is currently active.
    """
    res = None
    
    try:
        res = requests.get("http://httpbin.org/ip", proxies={"http": f"http://{proxy}"}, timeout=1.8)
        if res.status_code >= 400:
            res = None
        else:
            print("proxy ok", proxy)
            if save_active_proxies:
                with open(ACTIVE_PROXIES_PATH, 'a') as f:
                    f.write(proxy + '\n')
    except Exception as e:
        print(e)
    return res is not None


def get_active_proxies(refresh=False):
    """
    Iterates through a list of proxies, and returns those that are currently active.
    """
    print("Checking for active proxies:")
    proxies_path = PROXIES_PATH if refresh else ACTIVE_PROXIES_PATH
    with open(proxies_path, 'r') as f:
        return list(filter(proxy_is_active, f.read().strip().split('\n')))


def blockcypher_fetch(addr, proxy=None):
    """
    Makes a call to the Blockcypher API with the given Bitcoin address, 
    and returns the result together with its associated status code.
    """
    assert is_valid_btc_address(addr)
    
    url = f"http://api.blockcypher.com/v1/btc/main/addrs/{addr}/balance"
    try:
        res = requests.get(url, proxies={"http": f"http://{proxy}"} if proxy else None, timeout=5)
        print(res.status_code, res.json())
        return (res.json(), res.status_code)
    except Exception as e:
        print(e)
        return (e, 0)


def blockcypher_fetch_all(addr_path, output_path="blockcypher.txt"):
    """
    Iterates through a list of Bitcoin addresses, calls the Blockcypher API for each, 
    and saves successful requests to a file.
    """
    proxies = get_active_proxies()
    
    with open(addr_path) as f:
        for i, btc_addr in enumerate(f.readlines()):
            print(f'\n{i}:', btc_addr.strip())
            
            while True:
                if not proxies:
                    print("All proxies reached the API limit.")
                    return
                
                proxy = random.choice(proxies)
                print("using proxy", proxy)
                res, status_code = blockcypher_fetch(btc_addr.strip(), proxy=proxy)
                
                if status_code == 200:
                    with open(output_path, 'a') as f:
                        json.dump(res, f)
                        f.write('\n')
                elif status_code == 429: # API limit reached
                    print(proxy, "API limit reached")
                    proxies.remove(proxy)
                    continue
                break
            

def blockchain_fetch(addr, proxy=None):
    """
    Makes a call to the Blockchain API with the given Bitcoin address, 
    and returns the result together with its associated status code.
    """
    assert is_valid_btc_address(addr)
    
    url = f"https://blockchain.info/rawaddr/{addr}"
    try:
        res = requests.get(url, proxies={"http": f"http://{proxy}"}, timeout=5)
        print(res.status_code, res.json())
        return (res.json(), res.status_code)
    except Exception as e:
        print(e)
        return (e, 0)
        
    
def blockchain_fetch_all(addr_path, output_path="blockchain.txt"):
    """
    Iterates through a list of Bitcoin addresses, calls the Blockchain API for each, 
    and saves successful requests to a file.
    """
    proxies = get_active_proxies()
    
    with open(addr_path) as f:    
        for i, btc_addr in enumerate(f.readlines()):
            print(f'\n{i}:', btc_addr.strip())
            
            while True:
                if not proxies:
                    print("All proxies reached the API limit.")
                    return
                
                proxy = random.choice(proxies)
                print("using proxy", proxy)
                res, status_code = blockchain_fetch(btc_addr.strip(), proxy=proxy)
                
                if status_code == 200: 
                    with open(output_path, 'a') as f:
                        json.dump(res, f)
                        f.write('\n')
                elif status_code == 429:
                    print(proxy, "API limit reached")
                    proxies.remove(proxy)
                    continue
                break
