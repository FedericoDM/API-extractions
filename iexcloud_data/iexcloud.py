# Imports
import requests
import pandas as pd
from datetime import datetime

# Local imports
from keys import iexcloud_keys

# Parameters
token = iexcloud_keys["token"]
keys_to_delete = ["latestUpdate", "latestPrice"]

# Functions


def request_ticker_data(base_url, ticker, token):
    """
    Gets the latest price, open, high, and low for a given ticker.

    Parameters
    ----------
    base_url : str
        base url for request
    ticker : str
        ticker
    token : str
        IEXCloud token

    Returns
    -------
    dict
        Dictionary data
    """
    quote_path = f"/stable/stock/{ticker}/quote?token={token}&filter=latestUpdate,open,high,low,latestPrice"
    url = base_url + quote_path
    ticker_dict = requests.get(url).json()
    return ticker_dict


def format_dict(ticker_dict, ticker):
    """
    Formats the dictionary data from request_ticker_data().
    """

    epoch = ticker_dict["latestUpdate"]
    s = epoch / 1000.0
    updated = datetime.fromtimestamp(s).strftime("%Y-%m-%d %H:%M:%S")
    date = datetime.fromtimestamp(s).strftime("%Y-%m-%d")

    ticker_dict["updated"] = updated
    ticker_dict["date"] = date
    ticker_dict["ticker"] = ticker
    ticker_dict["last"] = ticker_dict["latestPrice"]

    for key_to_delete in keys_to_delete:
        del ticker_dict[key_to_delete]

    return ticker_dict


# PIPELINE

# Aqui los leo de un csv, pero esto puede cambiar y que sea de la db
active_tickers = pd.read_csv("active_tickers.csv", encoding="utf-8-sig")

# Lista de tickers y esignals
tickers = active_tickers["Symbol"].tolist()
esignals = active_tickers["eSignal"].tolist()

# Sacar datos de cada uno de los tickers
ticker_dicts = []
for esignal, ticker in zip(esignals, tickers):
    try:
        ticker_dict = request_ticker_data("https://cloud.iexapis.com", esignal, token)
        clean_dict = format_dict(ticker_dict, ticker)
        ticker_dicts.append(clean_dict)
    except Exception as e:
        print(f"Error with {esignal}, error: {e}")


# Formatting dataframe
df = pd.DataFrame(ticker_dicts)
df = df[["ticker", "date", "open", "high", "low", "last", "updated"]]

# TO DO:  Aqui faltaria la parte de subir/reemplazar los datos en la db
