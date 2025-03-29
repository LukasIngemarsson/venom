import pandas as pd
import json
from collections import defaultdict


def save_btc_addrs_from_crawler(crawler_data_path="data.txt", 
                                output_path="btc-addrs.txt"
                                ):
    """
    Extracts the set of unique Bitcoin addresses from a given crawler data file,
    and outputs each address to the specified output path.
    """
    with open(crawler_data_path, 'r') as f:
        onion_to_data = eval(f.read())

    btc_addrs = set()
    with open(output_path, 'a') as f:
        for v in onion_to_data.values():
            if isinstance(v, dict):
                for addr in v["btc_addrs"]:
                    if addr not in btc_addrs:
                        btc_addrs.add(addr)
                        f.write(addr + '\n')
                        

def save_valid_btc_addrs(blockstream_data_path="blockstream.txt", 
                         output_path="valid-btc-addrs.txt"
                         ):
    """
    Extracts the set of valid unique Bitcoin addresses from a Blockstream data file,
    and outputs each address to the specified output path.
    """
    with open(blockstream_data_path, 'r') as f:
        valid_btc_addrs = [eval(res)["address"] for res in f.readlines()]
    
    with open(output_path, 'a') as f:
        for addr in valid_btc_addrs:
            f.write(addr + '\n')
            

def consolidate_btc_data(blockstream_data_path="blockstream.txt", 
                         output_path="btc-data.txt"
                         ):
    """
    Extracts the chain stats (meta data) for each Bitcoin address in the provided
    Blockstream data file, and outputs it to the specified output path.
    """
    addr_info = dict()
    with open(blockstream_data_path, 'r') as f:
        for res in f.readlines():
            res = eval(res)
            addr_info[res["address"]] = res["chain_stats"]
    with open(output_path, 'a') as f:
        json.dump(addr_info, f)
        

def categorize_by_title(title):
    """
    Takes a website's title and returns a string containing the topics derived from the title.
    """

    # Note: Some topic keywords are intentionally odd, e.g., 
    # "multipl" will recognize both "multiply" and "multiplier"
    topic_keywords = ["bitcoin", "btc", "bit", "mixer", 
                      "miner", "double", "triple", "multipl",
                      "private key", "generat", "wallet", 
                      "card", "paypal", "western union", "hack", 
                      "money", "cash", "transfer", "buy", "market", "sell",
                      "sale", "list", "link", "dir", "fuck", "porn", "cp", 
                      "rape", "pedo", "teen", "child", "underage", "young"]
    title = title.lower()

    matches = set()
    for kw in topic_keywords:
        if kw in title:
            matches.add(kw)

    def has_match(keywords):
        return any(kw in matches for kw in keywords)

    """
    Topics:
        bitcoin_mixer
        bitcoin_generator
        stolen_bitcoin
        hacking
        stolen_funds
        market
        website_list
        pornography
        abusive_content
        other
    """

    topics = []    
    if has_match(("bitcoin", "btc", "bit")):
        if has_match(("mixer",)):
            topics.append("bitcoin_mixer")

        if has_match(("generat", "miner", "double", "triple", "multipl")):
            topics.append("bitcoin_generator")

        if has_match(("wallet", "hack", "private key")):
            topics.append("stolen_bitcoin")
    elif has_match(("hack",)):
        topics.append("hacking")
        
    if has_match(("card", "paypal", "western union", "transfer", "money", "cash")):
        topics.append("stolen_funds")

    if has_match(("market", "buy", "sale", "sell")):
        topics.append("market")

    if has_match(("list", "link", "dir")):
        topics.append("website_list")

    if has_match(("porn", "fuck")):
        topics.append("pornography")

    if has_match(("cp", "rape", "pedo", "teen", "child", "underage", "young")):
        topics.append("abusive_content")

    if not topics:
        topics.append("other")
    
    return ", ".join(topics)


def create_dataset(crawler_data_path="data.txt",
                   btc_data_path="btc-data.txt",
                   output_path="dataset.csv",
                   full=True # set to False if you want to cleanly view the file in Excel
                   ):    
    """
    Takes in a crawler data file and an associated Bitcoin data file, and outputs
    the consolidated dataset to the specified output path.
    """               
    with open(crawler_data_path, 'r') as f:
        onion_to_data = eval(f.read())
    
    with open(btc_data_path, 'r') as f:
        btc_addr_info = eval(f.read())
    
    dataset_rows = []
    for k, v in onion_to_data.items():
        if "ahmia.fi" in k: # exclude Ahmia seeds
            continue
        
        row = defaultdict(None, {"onion_addr": k,
                                 "title": None,
                                 "topic": None,
                                 "btc_addrs": None,
                                 "btc_addrs_count": None,
                                 "total_sent": None,
                                 "total_received": None,
                                 "n_tx": None,
                                 "comment": None
                                 })
        
        if isinstance(v, dict):
            row["title"] = v["title"].replace('\r', '').replace('\n', '')
                
            row["btc_addrs"] = [a for a in v["btc_addrs"] if a in btc_addr_info.keys()]

            if row["btc_addrs"]:
                row["btc_addrs_count"] = len(row["btc_addrs"])
                row["btc_addrs"] = sorted(row["btc_addrs"],
                                          reverse=True,
                                          key=lambda x: btc_addr_info[x]["spent_txo_sum"] +\
                                                        btc_addr_info[x]["funded_txo_sum"])
                            
                row["total_sent"], row["total_received"], row["n_tx"] = 0, 0, 0
                for addr in row["btc_addrs"]:
                    row["total_sent"] += btc_addr_info[addr]["spent_txo_sum"]
                    row["total_received"] += btc_addr_info[addr]["funded_txo_sum"]
                    row["n_tx"] += btc_addr_info[addr]["tx_count"]
                    
                row["total_sent"] = row["total_sent"] / 10**8
                row["total_received"] = row["total_received"] / 10**8
                            
                if not full and len(str(row["btc_addrs"])) > 32000: # exceeds Excel cell limit
                    trunc_btc_addrs = row["btc_addrs"]

                    while len(str(trunc_btc_addrs)) > 32000:
                        trunc_btc_addrs.pop()

                    row["comment"] = f"""Too many addresses to display in Excel;
                                        {len(trunc_btc_addrs)} of {len(row["btc_addrs"])} shown."""
                    
            if row["title"]:
                row["topic"] = categorize_by_title(row["title"])
        else:
            row["comment"] = v

        dataset_rows.append(row)

    pdf = pd.DataFrame.from_records(dataset_rows)
    
    pdf.to_csv(output_path, sep=';', decimal=',', index=False)