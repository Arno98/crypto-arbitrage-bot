import os
import re
import requests
import threading
import time
import asyncio
import concurrent.futures
import json
import telebot
from threading import Thread
import hmac
import hashlib
import base64
from datetime import datetime, timezone
from rapidfuzz import process, fuzz
from dotenv import load_dotenv

load_dotenv()


bot = telebot.TeleBot(os.getenv("BOT_TOKEN"))

api_key_binance = os.getenv("BINANCE_API_KEY")
secret_key_binance = os.getenv("BINANCE_SECRET_KEY")
api_key_okx = os.getenv("OKX_API_KEY")
secret_key_okx = os.getenv("OKX_SECRET_KEY")
api_key_bybit = os.getenv("BYBIT_API_KEY")
secret_key_bybit = os.getenv("BYBIT_SECRET_KEY")
api_key_mexc = os.getenv("MEXC_API_KEY")
secret_key_mexc = os.getenv("MEXC_SECRET_KEY")
api_key_bingx = os.getenv("BINGX_API_KEY")
secret_key_bingx = os.getenv("BINGX_SECRET_KEY")

capitalization = 50000.00


def name_normalization(symbols_prices):
    updated_symbols_prices = {}
    
    for symbol, price in symbols_prices.items():
        base_currency, quote_currency = None, None
        for currency in ('USDT', 'USD', 'EUR', 'USDC', 'BTC', 'ETH', 'LTC', 'BNB', 'DOGE', 'GBP', 'BUSD'):
            if symbol.endswith(currency):
                base_currency = symbol[:-len(currency)]
                quote_currency = currency
                break

        if base_currency is not None and quote_currency is not None:
            updated_symbol = f"{base_currency}-{quote_currency}"
            updated_symbols_prices[updated_symbol] = price

    return updated_symbols_prices

def get_prices_binance(response):
    timestamp_binance = int(time.time() * 1000)
    message_binance = f'timestamp={timestamp_binance}'
    signature_binance = hmac.new(secret_key_binance.encode(), message_binance.encode(), hashlib.sha256).hexdigest()

    dw_response = requests.get("https://api.binance.com/sapi/v1/capital/config/getall?timestamp=" + str(timestamp_binance) + "&" + "signature=" + signature_binance, headers={"X-MBX-APIKEY": api_key_binance})
    dw_binance = {item['coin']: [network['name'] for network in item['networkList']] for item in dw_response.json() if any(network['depositEnable'] and network['withdrawEnable'] for network in item['networkList'])}
    
    symbols_prices_usdt_eur = {n['symbol']: [float(n['askPrice']), float(n['bidPrice']), float(n['quoteVolume']), float(n["askQty"]), float(n["bidQty"])] for n in response.json() if n['symbol'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and float(n['quoteVolume']) > capitalization}
    symbols_prices_usdt_eur1 = {key: value for key, value in name_normalization(symbols_prices_usdt_eur).items() if key.split('-')[0] in dw_binance}
    
    return symbols_prices_usdt_eur1, dw_binance
    
def get_prices_kraken(response):
    response1 = requests.get('https://api.kraken.com/0/public/Assets')
    dw_kraken = [value['altname'] for value in response1.json()['result'].values() if value['status'] == 'enabled']

    usdt_eur_symbols_prices1 = {key: [float(value['a'][0]), float(value['b'][0]), float(value['c'][0]) * float(value['v'][1]), float(value['a'][2]), float(value['b'][2])] for key, value in response.json()['result'].items() if key.endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and (float(value['c'][0]) * float(value['v'][1])) > capitalization}
    symbols_prices_usdt_eur1 = {key: value for key, value in name_normalization(usdt_eur_symbols_prices1).items() if key.split('-')[0] in dw_kraken}
  
    return symbols_prices_usdt_eur1, 1

def get_prices_bitstamp(response):
    resp_bitstamp = requests.get("https://www.bitstamp.net/api/v2/ticker/")
    trading_symbol_bitstamp = {d['name'] for d in response.json() if d['trading'] == 'Enabled' and d['instant_and_market_orders'] == 'Enabled'}
    
    response1 = requests.get('https://www.bitstamp.net/api/v2/currencies/')
    dw_bitstamp = [d['currency'] for d in response1.json() if d['deposit'] and d['withdrawal']]
    
    usd_eur_usdt_symbols_prices1 = {d['pair'].replace('/', '-'): [float(d['ask']), float(d['bid']), float(d['last']) * float(d['volume']), 0.0, 0.0] for d in resp_bitstamp.json() if d['pair'] in trading_symbol_bitstamp and d['pair'].split('/')[0] in dw_bitstamp and d['pair'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and (float(d['last']) * float(d['volume'])) > capitalization}
    return usd_eur_usdt_symbols_prices1, 1
    
def get_prices_bitfinex(response):
    usd_eur_symbols_prices1 = {d[0].replace('t', '').replace(':', '').replace('TEST', ''): [float(d[3]), float(d[1]), float(d[7]) * float(d[8]), float(d[2]), float(d[4])] for d in response.json() if d[0].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and (float(d[7]) * float(d[8])) > capitalization and not d[0].endswith(('AMPUSD', 'AMPUST', 'AMPBTC', 'USTUSD'))}
    return name_normalization(usd_eur_symbols_prices1), 1

def get_prices_okx(response):
    timestamp_okx = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    message_okx = timestamp_okx + 'GET/api/v5/asset/currencies'
    signature_okx = hmac.new(secret_key_okx.encode(), message_okx.encode(), hashlib.sha256).digest()
    encoded_signature_okx = base64.b64encode(signature_okx).decode()

    response1 = requests.get("https://www.okx.com/api/v5/asset/currencies", headers={"OK-ACCESS-KEY": api_key_okx, "OK-ACCESS-SIGN": encoded_signature_okx, "OK-ACCESS-TIMESTAMP": timestamp_okx, "OK-ACCESS-PASSPHRASE": "BSSgUyC#rv6Utb?"})
    dw_okx = {item['ccy']: [chain['chain'].split('-')[1] for chain in response1.json()['data'] if chain['ccy'] == item['ccy']] for item in response1.json()['data'] if item['canDep'] and item['canWd']}
	
    usdt_eur_prices_symbols = {data['instId']: [float(data['askPx']), float(data['bidPx']), float(data['volCcy24h']), float(data["askSz"]), float(data["bidSz"])] for data in response.json()['data'] if data['instId'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and data['instId'].split('-')[0] in dw_okx and float(data['volCcy24h']) > capitalization}
    
    return usdt_eur_prices_symbols, dw_okx 
    
def get_prices_bybit(response):
    timestamp_bybit = int(time.time() * 1000)
    message_bybit = str(timestamp_bybit) + api_key_bybit
    signature_bybit = hmac.new(secret_key_bybit.encode("utf-8"), message_bybit.encode("utf-8"), hashlib.sha256).hexdigest()

    response1 = requests.get("https://api.bybit.com/v5/asset/coin/query-info", headers={"X-BAPI-TIMESTAMP": str(timestamp_bybit), "X-BAPI-API-KEY": api_key_bybit, "X-BAPI-SIGN": signature_bybit})
    
    dw_bybit = {item['coin']: [network['chainType'] for network in item['chains']] for item in response1.json()['result']['rows'] if any(network['chainDeposit'] == '1' and network['chainWithdraw'] == '1' for network in item['chains'])}
    
    usdt_eur_prices_symbols = {data['symbol']: [float(data['ask1Price']), float(data['bid1Price']), float(data['lastPrice']) * float(data['volume24h']), float(data["ask1Size"]), float(data["bid1Size"])] for data in response.json()['result']['list'] if data['symbol'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) if (float(data['lastPrice']) * float(data['volume24h'])) > capitalization}
    symbols_prices_usdt_eur1 = {key: value for key, value in  name_normalization(usdt_eur_prices_symbols).items() if key.split('-')[0] in dw_bybit}
    
    return symbols_prices_usdt_eur1, dw_bybit
    
def get_prices_gateio(response):
    response1 = requests.get('https://api.gateio.ws/api/v4/spot/currencies')
    dw_gateio = {d['currency']: [d['chain']] for d in response1.json() if not d["deposit_disabled"] and not d["withdraw_disabled"] and not d["withdraw_delayed"]}

    usdt_eur_prices_symbols = {x["currency_pair"].replace('_', '-').upper(): [float(x['lowest_ask']), float(x['highest_bid']), float(x['quote_volume']), 0.0, 0.0] for x in response.json() if x['currency_pair'].replace('_', '-').upper().endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and x["currency_pair"].split('_')[0].upper() in dw_gateio and float(x['quote_volume']) > capitalization}
    return usdt_eur_prices_symbols, dw_gateio
    
def get_prices_bitget(response):
    response1 = requests.get("https://api.bitget.com/api/v2/spot/public/coins")
    dw_bitget = {coin_data['coin']: [chain['chain'] for chain in coin_data['chains']] for coin_data in response1.json()['data'] if any(chain["withdrawable"] == "true" and chain["rechargeable"] == "true" for chain in coin_data['chains'])}

    usdt_eur_prices_symbols = {d['symbol'].split('_')[0]: [float(d['bestAsk']), float(d['bestBid']), float(d['quoteVolume']), float(d["askSz"]), float(d["bidSz"])] for d in response.json()['data'] if d['symbol'].split('_')[0].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and float(d['quoteVolume']) > capitalization}

    symbols_prices_usdt_eur1 = {key: value for key, value in name_normalization(usdt_eur_prices_symbols).items() if key.split('-')[0] in dw_bitget}
    return symbols_prices_usdt_eur1, dw_bitget
    
def get_prices_mexc(response):
    timestamp_mexc = int(time.time() * 1000)
    message_mexc = f'timestamp={timestamp_mexc}'
    signature_mexc = hmac.new(secret_key_mexc.encode(), message_mexc.encode(), hashlib.sha256).hexdigest()

    response1 = requests.get("https://api.mexc.com/api/v3/capital/config/getall?timestamp=" + str(timestamp_mexc) + "&" + "signature=" + signature_mexc, headers={"X-MEXC-APIKEY": api_key_mexc})
    
    dw_mexc = {item['coin']: [network['network'] for network in item['networkList']] for item in response1.json() if any(network["depositEnable"] and network["withdrawEnable"] for network in item['networkList'])}
    usdt_eur_prices_symbols = {d['symbol']: [float(d['askPrice']), float(d['bidPrice']), float(d['quoteVolume']), float(d["askQty"]), float(d["bidQty"])] for d in response.json() if d['symbol'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and float(d['quoteVolume']) > capitalization}
    
    symbols_prices_usdt_eur1 = {key: value for key, value in name_normalization(usdt_eur_prices_symbols).items() if key.split('-')[0] in dw_mexc}
    return symbols_prices_usdt_eur1, dw_mexc
    
def get_prices_htx(response):
    response1 = requests.get("https://api.huobi.pro/v2/reference/currencies")
    dw_htx = {item['currency'].upper(): [network["displayName"] for network in item['chains']] for item in response1.json()['data'] if any(network["withdrawStatus"] == "allowed" and network["depositStatus"] == "allowed" for network in item['chains'])}

    symbols_prices_usdt_eur = {d['symbol'].upper(): [d['ask'], d['bid'], d["vol"], d["askSize"], d["bidSize"]] for d in response.json()['data'] if d['symbol'].upper().endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and d["vol"] > capitalization}
    symbols_prices_usdt_eur1 = {key: value for key, value in name_normalization(symbols_prices_usdt_eur).items() if key.split('-')[0] in dw_htx}
    
    return symbols_prices_usdt_eur1, dw_htx
    
def get_prices_cryptocom(response):
    symbols_prices_usdt_eur1 = {d['i'].replace('_', '-'): [float(d['k']), float(d['b']), float(d["vv"]), 0.0, 0.0] for d in response.json()['result']['data'] if d['i'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC'})) and float(d["vv"]) > capitalization}

    return symbols_prices_usdt_eur1, 1
    
def get_prices_bingx(response):
    timestamp_bingx = int(time.time() * 1000)
    message_bingx = f'timestamp={timestamp_bingx}'
    signature_bingx = hmac.new(secret_key_bingx.encode(), message_bingx.encode(), hashlib.sha256).hexdigest()

    response1 = requests.get("https://open-api.bingx.com/openApi/wallets/v1/capital/config/getall?timestamp=" + str(timestamp_bingx) + "&" + "signature=" + signature_bingx, headers={"X-BX-APIKEY": api_key_bingx})
    dw_bingx = {item['coin']: [network['network'] for network in item['networkList']] for item in response1.json()['data'] if any(network["depositEnable"] and network["withdrawEnable"] for network in item['networkList'])}
    
    response3 = requests.get("https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?timestamp=" + str(timestamp_bingx) + "&" + "signature=" + signature_bingx, headers={"X-BX-APIKEY": api_key_bingx})
    symbols_prices_usdt_eur1 = {d['symbol']: [float(d.get('askPrice', 0.0)), float(d.get('bidPrice', 0.0)), float(d['quoteVolume']), float(d.get('askQty', 0.0)), float(d.get('bidQty', 0.0))] for d in response3.json().get('data', []) if d['symbol'].endswith(tuple({'USD', 'USDT', 'EUR', 'USDC', 'BTC', 'ETH'})) and float(d['quoteVolume']) > capitalization and d['symbol'] in dw_bingx}
    
    return symbols_prices_usdt_eur1, dw_bingx

def get_prices(exchange, api_url=None):
    price_functions = {
        "Binance": get_prices_binance,
        "Kraken": get_prices_kraken,
        "Bitstamp": get_prices_bitstamp,
        "Bitfinex": get_prices_bitfinex,
        "OKX": get_prices_okx,
        "Bybit": get_prices_bybit,
        "Gate.io": get_prices_gateio,
        "Bitget": get_prices_bitget,
        "MEXC": get_prices_mexc,
        "HTX": get_prices_htx,
        "Crypto.com": get_prices_cryptocom,
        "BingX": get_prices_bingx,
    }
    
    response = requests.get(api_url)
    return price_functions[exchange](response)

def compare_prices():
    
    def find_common_networks_for_coin(dict1, dict2, coin, threshold=50):
        try:
            if coin in dict1 and coin in dict2:
                networks1, networks2 = dict1[coin], dict2[coin]
                matched_networks = {n1 for n1 in networks1 for n2, score, _ in process.extract(n1, networks2, scorer=fuzz.partial_ratio, limit=None) if score >= threshold}
                return ', '.join(matched_networks) if matched_networks else "Use crosschain bridge for transaction."
        except TypeError:
             return "No data about networks."

    message = ""
    exchanges = ["Binance", "Kraken", "Bitstamp", "Bitfinex", "OKX", "Bybit", "Gate.io", "Bitget", "MEXC", "HTX", "Crypto.com", "BingX"]
    exchange_api_urls = {
        "Binance": "https://api.binance.com/api/v3/ticker/24hr",
        "Kraken": "https://api.kraken.com/0/public/Ticker",
        "Bitstamp": "https://www.bitstamp.net/api/v2/trading-pairs-info/",
        "Bitfinex": "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL",
        "OKX": "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
        "Bybit": "https://api.bybit.com/v5/market/tickers?category=spot",
        "Gate.io": "https://api.gateio.ws/api/v4/spot/tickers",
        "Bitget": "https://api.bitget.com/api/mix/v1/market/tickers?productType=umcbl",
        "MEXC": "https://api.mexc.com/api/v3/ticker/24hr",
        "HTX": "https://api.huobi.pro/market/tickers",
        "Crypto.com": "https://api.crypto.com/exchange/v1/public/get-tickers",
        "BingX":  "https://open-api.bingx.com/openApi/spot/v1/ticker/price",
    }
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(get_prices, exchanges, exchange_api_urls.values()))
        exchange_prices, exchange_chains = dict(zip(exchanges, [result[0] for result in results])),  dict(zip(exchanges, [result[1] for result in results]))


    all_symbols = set().union(*(data.keys() for data in exchange_prices.values()))

    exchanges_links = {
                       'Binance': f"https://www.binance.com/en/trade/%s_%s?type=spot",
                       'Bitfinex': f"https://trading.bitfinex.com/t/%s:%s?type=exchange",
					   'Bitget': f"https://www.bitget.com/spot/%s%s",
					   'Bitstamp': f"https://www.bitstamp.net/trade/%s/%s/",
					   'Bybit': f"https://www.bybit.com/en-US/trade/spot/%s/%s",
					   'Gate.io': f"https://www.gate.io/trade/%s_%s",
					   'Kraken': f"https://pro.kraken.com/app/trade/%s-%s",
					   'MEXC': f"https://www.mexc.com/uk-UA/exchange/%s_%s?_from=header",
					   'OKX': f"https://www.okx.com/en/trade-spot/%s-%s",
					   'HTX': f"https://www.htx.com/trade/%s_%s",
					   'Crypto.com': f"https://crypto.com/exchange/trade/%s_%s",
					   'BingX': f"https://bingx.com/en-us/spot/%s%s/",
					  }
    
    fees = {'Binance': 0.1, 'Kraken': 0.2,'Bitstamp': 0.4, 'Bitfinex': 0.2, 'OKX': 0.1, 'Bybit': 0.1,
                'Gate.io': 0.1, 'Bitget': 0.1, 'MEXC': 0.1, 'HTX': 0.2,  'Crypto.com': 0.1, 'BingX': 0.1,}      

    for symbol in all_symbols:
        ask_prices = [data[symbol][0] for exchange, data in exchange_prices.items() if symbol in data and data[symbol][0] != 0.0]
        bid_prices = [data[symbol][1] for exchange, data in exchange_prices.items() if symbol in data and data[symbol][1] != 0.0]
    
        if len(ask_prices) < 2 or len(bid_prices) < 2:
            continue
    
        min_price_ask, max_price_bid = min(map(float, ask_prices)), max(map(float, bid_prices))
    
        best_buy_exchange = next((exchange for exchange, data in exchange_prices.items() if symbol in data and float(data[symbol][0]) == min_price_ask), None)
        best_sell_exchange = next((exchange for exchange, data in exchange_prices.items() if symbol in data and float(data[symbol][1]) == max_price_bid), None)
    
        if best_buy_exchange is None or best_sell_exchange is None or best_buy_exchange == best_sell_exchange:
            continue
            
        profit_percent_ask_bid = ((max_price_bid - min_price_ask) / min_price_ask * 100) - (fees[best_buy_exchange] + fees[best_sell_exchange])
        
        if profit_percent_ask_bid < 0.5 or profit_percent_ask_bid > 20:
            continue

        best_buy_exchange_link = exchanges_links[best_buy_exchange] % (symbol.split('-')[0], symbol.split('-')[1]) if best_buy_exchange not in ['XT', 'HTX'] else exchanges_links[best_buy_exchange] % (symbol.split('-')[0].lower(), symbol.split('-')[1].lower())
        best_sell_exchange_link = exchanges_links[best_sell_exchange] % (symbol.split('-')[0], symbol.split('-')[1]) if best_sell_exchange not in ['XT', 'HTX'] else exchanges_links[best_sell_exchange] % (symbol.split('-')[0].lower(), symbol.split('-')[1].lower())
   
        common_networks = find_common_networks_for_coin(exchange_chains[best_buy_exchange], exchange_chains[best_sell_exchange], symbol.split('-')[0])
        volume_bbe, volume_bse = exchange_prices[best_buy_exchange][symbol][2], exchange_prices[best_sell_exchange][symbol][2]
    
        message += f"🔸<b>{symbol}</b>\n\n<b>{best_buy_exchange} ▶️ {best_sell_exchange}</b>\n\n<b>{min_price_ask}<a href=\"{best_buy_exchange_link}\"> (buy)</a> ⏩ {max_price_bid}<a href=\"{best_sell_exchange_link}\"> (sell)</a></b>\n\nVolume (24h): <b>${volume_bbe:.2f}</b> | <b>${volume_bse:.2f}</b>\n\n🔗 Networks: <b>{common_networks}</b>\n\nPotential profit: <b>{profit_percent_ask_bid:.2f}%</b>\n\n\n"

    return message

pattern = re.compile(r'Potential profit: \d+\.\d+%')

def send_messages(messages, chat_id):
    for message in messages:
        bot.send_message(chat_id, message, parse_mode="html")

@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, 'Start scan...')
    msg = compare_prices()

    if len(msg) > 4096:
        messages = []
        start = 0
        while start < len(msg):
            end = start + 4096 if start + 4096 < len(msg) else len(msg)
            matches = list(pattern.finditer(msg, start, end))
            if matches:
                last_full_entry = matches[-1].end()
            else:
                last_full_entry = end
            messages.append(msg[start:last_full_entry].strip())
            start = last_full_entry
            if start == end:
                start += 4096
        
        thread = Thread(target=send_messages, args=(messages, message.chat.id))
        thread.start()
    else:
        bot.send_message(message.chat.id, msg, parse_mode="html")

bot.infinity_polling()
