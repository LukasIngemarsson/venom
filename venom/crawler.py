import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import threading
from time import time
from math import inf
import os
from collections import deque


class BaseCrawler:
    """
    The base crawler implementation.
    """
    def __init__(self, 
                 seeds, 
                 search_limit=inf, 
                 output_dir="",
                 resume_path=None,
                 ):
        """    
        Args:
            seeds: A list of seed addresses.
            search_limit: A limit for the total number of addresses to crawl.
            output_dir: A directory where the output of the crawler should be saved.
            resume_path: A path to a savestate file to resume crawling from.
        """
        self.seeds = seeds
        self.search_limit = search_limit
        self.output_dir = output_dir
        self.resume_path = resume_path

        self.data_path = os.path.join(self.output_dir, "data.txt")
        self.log_path = os.path.join(self.output_dir, "log.txt")
        self.savestate_path = os.path.join(self.output_dir, "savestate.txt")

    def _setup_structs(self):
        if self.resume_path:
            self._load_prev_crawl()
        else:
            self.queue = deque([x for x in self.seeds])
            self.searched = set()
     
    def _on_shutdown(self):        
        print("Exiting crawler.") 
        
        with open(self.savestate_path, 'w') as f:
            f.write(f"{self.queue}\n{self.searched}")   
            
    def _load_prev_crawl(self):
        with open(self.resume_path, 'r') as f:
            self.queue = eval(f.readline())
            self.searched = eval(f.readline())
    
    def _write_data(self, text):
        with open(self.data_path, 'a') as f:
            f.write(text)
            
    def _write_log(self, text):
        with open(self.log_path, 'a') as f:
            f.write(text)
            
    def _is_new_address(self, addr):
        return addr not in self.queue and addr not in self.searched
        
    def _log_iter(self, url, result, start_time):
        self.searched.add(url)
        
        log_info = " | ".join([f"#{len(self.searched)}", url, 
                               f"In queue: {len(self.queue)}",
                               f"Render time: {round(time() - start_time, ndigits=2)} s",
                               result + '\n'])
        print(log_info)
        self._write_log(log_info)
        
    def _is_valid_com_address(self, addr):
        return addr and ".com" in addr
        
    def _scrape_links(self, bsoup):
        for a in bsoup.findAll('a'):
            href = a.get("href")
            if self._is_valid_com_address(href) and self._is_new_address(href):
                self.queue.append(href)
            
    def scrape_url(self, url):
        """
        Scrapes the given URL, enqueuing all links found on the page.
        """
        start_time = time()
        try:
            response = requests.get(url, timeout=3)
            
            self._log_iter(url=url, result=f"HTTP {response.status_code}", start_time=start_time)
            
        except Exception as e:
            self._log_iter(url=url, result=e.__class__.__name__, start_time=start_time)
            return
    
        self._write_data(url + '\n')
        
        if response.status_code == 200:
            bsoup = BeautifulSoup(response.text, "html.parser")
            self._scrape_links(bsoup)
        else:
            print(f"HTTP Error: {response.status_code}")
    
    def crawl(self):      
        """
        Runs a crawl, saving collected data and a log of the crawler's actions during its traversal, 
        as well as a savestate upon shutdown (containing the crawler's remaining queue and a set of all 
        the visited addresses). 
        
        Carefully use keyboard interrupt `^C` to initiate shutdown prematurely.
        """
        self._setup_structs() 
        print("Running", self.__class__.__name__, '\n')
        
        try:
            while self.queue:
                if len(self.searched) >= self.search_limit:
                    print("Search limit reached.")
                    break
                
                self.scrape_url(url=self.queue.popleft())

            self._on_shutdown()

        except KeyboardInterrupt:
            self._on_shutdown()
    

class DarkCrawler(BaseCrawler):    
    """
    The dark web crawler implementation. Builds upon the base crawler class.
    """
    def __init__(self, 
                 seeds=[], 
                 search_limit=inf, 
                 output_dir="", 
                 resume_path=None, 
                 ahmia_keyword_path=None, 
                 proxies=None, 
                 request_timeout_after=60
                 ):
        """    
        Args:
            seeds: A list of seed addresses.
            search_limit: A limit for the total number of addresses to crawl.
            output_dir: A directory where the output of the crawler should be saved.
            resume_path: A path to a savestate file to resume crawling from.
            ahmia_keyword_path: A path to a file containing keywords to search for
                on Ahmia (append to "https://ahmia.fi/search/?q=") and add as seed addresses.
            proxies: A dictionary containing the proxies to be used when crawling (e.g.,
                the Tor proxy to crawl onion addresses). Note that the service that the 
                proxy uses has to be running for this to work.
            request_timeout_after: A number in seconds after which a request to a website 
                should be timed out when crawling.
        """
        super().__init__(seeds, search_limit, output_dir, resume_path)
        self.ahmia_keyword_path = ahmia_keyword_path
        self.proxies = proxies
        self.request_timeout_after = request_timeout_after
    
    def _setup_structs(self):
        if self.resume_path:
            self._load_prev_crawl()
        else:
            self.queue = deque([x for x in self.seeds])
            self.searched = set()
            
            self._write_data('{' + '\n')
            
        if self.ahmia_keyword_path:
            self._load_ahmia_seeds()
                
    def _on_shutdown(self):        
        print("Exiting crawler.") 
        self._write_data('}' + '\n')
        
        with open(self.savestate_path, 'w') as f:
            f.write(f"{self.queue}\n{self.searched}")   
            
    def _load_prev_crawl(self):
        super()._load_prev_crawl()
        with open(self.output_path, 'r+') as f:
            lines = f.readlines()
            f.seek(0)
            f.truncate()
            
            for line in lines[:-1]:
                f.write(line)
        
    def _load_ahmia_seeds(self):
        ahmia_link = "https://ahmia.fi/search/?q="
        
        with open(self.ahmia_keyword_path, 'r') as f:
            for kw in f.readlines():
                kw_link = ahmia_link + kw.strip().replace(' ', '+')
                
                if kw_link not in self.searched:
                    self.queue.append(kw_link)
            
    def _is_new_onion_address(self, addr):
        return addr not in self.queue and addr not in self.searched
    
    def _scrape_links(self, bsoup):
        for a in bsoup.findAll('a'):
            onion_addr = self.onion_address_search(a.get("href"))

            if onion_addr and self._is_new_onion_address(onion_addr):
                self.queue.append(onion_addr)
                
    def _scrape_btc_addresses(self, bsoup, url):
        url_text = bsoup.get_text(separator='§').split('§')
        btc_addresses = set()
        
        links = set(a.get("href") for a in bsoup.findAll('a'))
                
        for elem_text in url_text:
            for text in elem_text.split():
                btc_addr = self.btc_address_search(text)
                
                if not btc_addr or btc_addr + ".onion" in links:
                    continue
                
                btc_addresses.add(btc_addr)
                
        out_value = {
                     "btc_addrs": list(btc_addresses),
                     "title": bsoup.title.string.replace('\n', '').strip()
                     }
        self._write_data(f"  '{url}': {str(out_value)},\n")
        
    def onion_address_search(self, text):
        """
        Finds onion addresses in the given string.
        """
        if text:
            onion_match = re.search(r"https?://\w+\.onion", text)
            if onion_match:
                return onion_match.group() 
    
    def btc_address_search(self, text):
        """
        Finds Bitcoin addresses in the given string.
        """
        if text:
            btc_match = re.fullmatch(r"(bc1|[13])[a-zA-Z0-9]{25,61}", text)
            if btc_match:
                return btc_match.group()
    
    def scrape_url(self, url):  
        """
        Scrapes the given URL, enqueuing all links found on the page.
        """
        start_time = time()
        try:
            # detect V2 addresses (<= 30): http[s]:// (7-8) + onion_addr (16) + .onion (6)
            if url.endswith(".onion") and len(url) <= 30:
                self._log_iter(url=url,
                               result=f"V2 address (deprecated)",
                               start_time=start_time)
                self._write_data(f"  '{url}': 'V2 address (deprecated)',\n")   
                return
            
            response = requests.get(url=url, proxies=self.proxies, timeout=self.request_timeout_after)
                    
            self._log_iter(url=url, result=f"HTTP {response.status_code}", start_time=start_time)
                        
        except Exception as e:            
            self._log_iter(url=url, result=e.__class__.__name__, start_time=start_time)
                    
            self._write_data(f"  '{url}': 'Exception: {e.__class__.__name__}',\n")            
            return
        
        if response.status_code == 200:
            bsoup = BeautifulSoup(response.text, "html.parser")
            
            self._scrape_links(bsoup)
            self._scrape_btc_addresses(bsoup, url)
        else:
            self._write_data(f"  '{url}': 'HTTP error: {response.status_code}',\n")            


class MultiThreadedDarkCrawler(DarkCrawler):    
    """
    The multithreaded dark web crawler implementation. Builds upon the dark web crawler class.
    """
    def __init__(self, 
                 seeds=[], 
                 search_limit=inf, 
                 output_dir="", 
                 resume_path=None, 
                 ahmia_keyword_path=None, 
                 proxies=None, 
                 request_timeout_after=60, 
                 nr_of_threads=5
                 ):
        """    
        Args:
            seeds: A list of seed addresses.
            search_limit: A limit for the total number of addresses to crawl.
            output_dir: A directory where the output of the crawler should be saved.
            resume_path: A path to a savestate file to resume crawling from.
            ahmia_keyword_path: A path to a file containing keywords to search for
                on Ahmia (append to "https://ahmia.fi/search/?q=") and add as seed addresses.
            proxies: A dictionary containing the proxies to be used when crawling (e.g.,
                the Tor proxy to crawl onion addresses). Note that the service that the 
                proxy uses has to be running for this to work.
            request_timeout_after: A number in seconds after which a request to a website 
                should be timed out when crawling.
            nr_of_threads: A number specifying the number of threads to be used when crawling.
                The limit of this parameter will depend on your hardware, and if you aim to 
                maximize it, the tuning will require some trial and error.
        """
        super().__init__(seeds, search_limit, output_dir, resume_path, 
                         ahmia_keyword_path, proxies, request_timeout_after)
        self.nr_of_threads = nr_of_threads
            
    def _setup_structs(self):
        self.seen = set()
        self.write_data_lock = threading.Lock()
        self.write_log_lock = threading.Lock()
        self.queue_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=self.nr_of_threads, 
                                           thread_name_prefix="Thread")    
        super()._setup_structs()     
            
    def _on_shutdown(self): 
        print("Shutting down. Waiting for tasks to finish.")
        self.executor.shutdown(cancel_futures=True)

        print("Exiting crawler.") 
        self._write_data('}' + '\n')
        
        with open(self.savestate_path, 'w') as f:
            f.write(f"{deque(self.seen.difference(self.searched))}\n{self.searched}")    

    def _load_prev_crawl(self):
        super()._load_prev_crawl()
        self.seen = set(self.queue).union(self.searched)
        
    def _is_new_onion_address(self, addr):
        return addr not in self.seen
        
    def _write_data(self, text):
        with self.write_data_lock:
            super()._write_data(text)
        
    def _write_log(self, text):        
        with self.write_log_lock:
            super()._write_log(text)
    
    def _log_iter(self, url, result, start_time):
        with self.log_lock:
            self.searched.add(url)
            log_info = " | ".join([f"#{len(self.searched)}", url,
                                    f"Called by: {threading.current_thread().name}",
                                    f"In queue: {self.executor._work_queue.qsize()}",
                                    f"Render time: {round(time() - start_time, ndigits=2)} s",
                                    result + '\n'])
            
            print(log_info)
            self._write_log(log_info)
                
    def _scrape_links(self, bsoup):
        for a in bsoup.findAll('a'):
            onion_addr = self.onion_address_search(a.get("href"))

            with self.queue_lock:
                if onion_addr and self._is_new_onion_address(onion_addr):
                    self.seen.add(onion_addr)
                    self.queue.append(onion_addr)
            
    def scrape_url(self, url):  
        """
        Scrapes the given URL, enqueuing all links found on the page.
        """
        start_time = time()
        try:
            # detect V2 addresses (<= 30): http[s]:// (7-8) + onion_addr (16) + .onion (6)
            if url.endswith(".onion") and len(url) <= 30:
                self._log_iter(url=url,
                               result=f"V2 address (deprecated)",
                               start_time=start_time)
                self._write_data(f"  '{url}': 'V2 address (deprecated)',\n")   
                return
            
            response = requests.get(url=url, proxies=self.proxies, timeout=self.request_timeout_after)
            
            self._log_iter(url=url, result=f"HTTP {response.status_code}", start_time=start_time)
            
        except Exception as e:
            self._log_iter(url=url, result=e.__class__.__name__, start_time=start_time)
            
            self._write_data(f"  '{url}': 'Exception: {e.__class__.__name__}',\n")
            return
        
        if response.status_code == 200:
            bsoup = BeautifulSoup(response.text, "html.parser")
            
            self._scrape_links(bsoup)
            self._scrape_btc_addresses(bsoup, url)
        else:
            self._write_data(f"  '{url}': 'HTTP error: {response.status_code}',\n")
        
    def crawl(self):
        """
        Runs a crawl, saving collected data and a log of the crawler's actions during its traversal, 
        as well as a savestate upon shutdown (containing the crawler's remaining queue and a set of all 
        the visited addresses).
        
        Carefully use keyboard interrupt `^C` to initiate shutdown prematurely 
        (it sometimes needs to be used more than once, so do so with caution until the 
        shutdown print message shows up to allow the crawler to properly shutdown). 
        """
        self._setup_structs()
        print("Running", self.__class__.__name__, '\n')

        try:
            with self.executor as exec:
                while True:
                    if len(self.searched) >= self.search_limit:
                        print("Search limit reached.")
                        self._on_shutdown()
                        break
                    
                    with self.queue_lock:
                        exec.map(self.scrape_url, self.queue)
                        self.queue = deque([])
                    
        except KeyboardInterrupt:
            self._on_shutdown()