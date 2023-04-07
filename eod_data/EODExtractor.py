import time
import requests
import pandas as pd

# Local imports
from keys import eod_keys


# EOD Price Extractor - Extracts EOD prices from EOD API
class EODExtractor:
    def __init__(self, ticker_path, output_path):

        # Global variables

        # Data path with tickers to use
        self.ticker_path = ticker_path
        self.tickers_df = pd.read_csv(self.ticker_path, encoding="utf-8-sig")

        # Threshold date
        self.threshold_date = "2000-01-01"

        # Output path
        self.output_path = output_path

        self.token = eod_keys["token"]
        self.tickers_url = f"https://eodhistoricaldata.com/api/exchange-symbol-list/EXCHANGE_CODE?api_token={self.token}&fmt=json"
        self.eod_url = f"https://eodhistoricaldata.com/api/eod/TICKER.COUNTRY?api_token={self.token}&period=d&fmt=json"
        self.adjusted_eod_url = f"https://eodhistoricaldata.com/api/technical/TICKER.COUNTRY?api_token={self.token}&fmt=json&function=splitadjusted"

        self.exchanges_of_interest = ["NYSE", "NYSE ARCA", "NASDAQ", "OTC"]
        self.types = ["Common Stock", "ETF"]

        self.missing_tickers_dict = {
            "CTRP": "NASDAQ",
            "DPK": "NYSE ARCA",
            "RUSS": "NYSE ARCA",
            "DGAZ": "NASDAQ",
            "PRSC": "NASDAQ",
            "IKNX": "NASDAQ",
            "RUSL": "NYSE ARCA",
            "BRK.B": "NYSE",
            "GASL": "NYSE ARCA",
            "TOT": "NYSE",
            "ESBK": "NASDAQ",
            "RALS": "NYSE ARCA",
            "pcy": "NYSE ARCA",
            "LBJ": "NYSE ARCA",
            "ICOL": "NYSE ARCA",
            "MIDZ": "NYSE ARCA",
            "KNOW": "NYSE ARCA",
            "AVGOP": "NASDAQ",
        }

        self.columns_map = {
            "ticker_id": "<TICKER>",
            "date": "<DATE>",
            "open": "<OPEN>",
            "high": "<HIGH>",
            "low": "<LOW>",
            "close": "<CLOSE>",
            "volume": "<VOL>",
        }

        self.ordered_cols = [
            "<TICKER>",
            "<PER>",
            "<DATE>",
            "<TIME>",
            "<OPEN>",
            "<HIGH>",
            "<LOW>",
            "<CLOSE>",
            "<VOL>",
            "<OPENINT>",
        ]

        self.undesired_dates = [
            "2014-11-27",
            "2015-01-19",
            "2015-02-16",
            "2018-11-22",
            "2019-02-18",
            "2023-01-16",
            "2018-07-04",
            "2018-09-03",
            "2018-11-22",
            "2018-12-05",
            "2019-01-21",
            "2019-02-18",
            "2019-05-27",
            "2019-07-04",
            "2019-09-02",
            "2019-11-28",
            "2020-01-20",
            "2020-02-17",
            "2020-05-25",
            "2020-07-03",
            "2020-09-07",
            "2020-11-26",
            "2021-01-18",
            "2021-02-15",
            "2021-05-31",
            "2021-07-05",
            "2021-09-06",
        ]

    # List of us symbols
    def get_us_symbols(self):
        """
        Get list of US symbols from EOD API
        by filtering on some conditions

        Parameters
        ----------
        token : str
            API token to use
        """
        url = f"https://eodhistoricaldata.com/api/exchange-symbol-list/US?api_token={self.token}&fmt=json"
        data = requests.get(url).json()
        df_us = pd.DataFrame(data)

        # Getting delisted tickers
        delisted_url = f"https://eodhistoricaldata.com/api/exchange-symbol-list/US?api_token={self.token}&fmt=json&delisted=1"
        data = requests.get(delisted_url).json()
        df_delisted = pd.DataFrame(data)

        # Concatenating dataframes
        df_us = pd.concat([df_us, df_delisted], ignore_index=True).reset_index(
            drop=True
        )
        # Filtering data
        esignals = self.tickers_df["esignal"].tolist()
        df_us = df_us[df_us["Code"].isin(esignals)].reset_index(drop=True)

        return df_us

    def get_tickers_list(exchange, default_url):
        """
        Gets list of tickers from EOD API for a given exchange

        Parameters
        ----------
        token : _type_
            _description_
        exchange : _type_
            _description_
        """

        new_url = default_url.replace("EXCHANGE_CODE", exchange)
        new_url = new_url + "&delisted=1"
        response = requests.get(new_url)
        data = response.json()

        return data

    def get_eod_df(self, ticker, country):
        """
        Gets JSON from the API and converts
        it to a dataframe

        Parameters
        ----------
        token : str
        ticker : str

        Returns
        -------
        DataFrame
            DataFrame with data
        """
        new_url = self.adjusted_eod_url.replace("TICKER", ticker).replace(
            "COUNTRY", country
        )
        response = requests.get(new_url)
        json_info = response.json()
        df = pd.DataFrame(json_info)
        df["ticker"] = ticker
        # Filtering by date
        # df = df[df["date"] >= self.threshold_date].reset_index(drop=True)
        return df

    # Converting dataframe to txt format
    def exchange_df_to_txt(self, merged_df, exchange, filename):
        """
        Converts dataframe to txt format

        Some parsing is done to make sure the data is in the correct format
        """
        # Place rare tickers in NYSE
        rare_tickers = ["SVXY", "USMV", "UVXY", "VIXM", "VIXY", "VXX", "VXZ"]
        merged_df.loc[merged_df["ticker"].isin(rare_tickers), "Exchange"] = "NYSE"

        # Place BATS and NYSE MKT in NYSE
        merged_df.loc[merged_df["Exchange"] == "BATS", "Exchange"] = "NYSE"
        merged_df.loc[merged_df["Exchange"] == "NYSE MKT", "Exchange"] = "NYSE"

        exchange_df = merged_df[merged_df["Exchange"] == exchange].reset_index(
            drop=True
        )
        if "adjusted_close" in exchange_df.columns:
            exchange_df.drop(
                columns=["ticker", "esignal", "Exchange", "adjusted_close"],
                inplace=True,
            )
        else:
            exchange_df.drop(columns=["ticker", "esignal", "Exchange"], inplace=True)
        exchange_df.rename(columns=self.columns_map, inplace=True)
        exchange_df["<PER>"] = "D"
        exchange_df["<TIME>"] = "000000"
        exchange_df["<DATE>"] = exchange_df["<DATE>"].apply(
            lambda x: x.replace("-", "")
        )
        exchange_df["<VOL>"] = exchange_df["<VOL>"].apply(lambda x: int(x))
        exchange_df["<OPENINT>"] = 0
        exchange_df = exchange_df[self.ordered_cols]
        exchange_df["<DATE>"] = exchange_df["<DATE>"].astype(int)
        exchange_df.sort_values(by=["<TICKER>", "<DATE>"], inplace=True)
        exchange_df.reset_index(drop=True, inplace=True)
        exchange_df.to_csv(filename, sep=",", index=False)

        print(f"Saved {exchange}.TXT")

        return exchange_df

    def get_eod_data(self):

        us_symbols = self.get_us_symbols()
        desired_tickers = self.tickers_df["esignal"].tolist()
        obtained_tickers = us_symbols["Code"].tolist()
        missing_tickers = list(set(desired_tickers) - set(obtained_tickers))

        # Get eod data for all tickers
        eod_df = pd.DataFrame()
        tickers = us_symbols["Code"].tolist() + list(self.missing_tickers_dict.keys())
        exchanges = us_symbols["Exchange"].tolist() + list(
            self.missing_tickers_dict.values()
        )

        counter = 0

        for ticker, exchange in zip(tickers, exchanges):
            try:
                df = self.get_eod_df(ticker, "US")
                df["Exchange"] = exchange
                eod_df = pd.concat([eod_df, df], ignore_index=True)
            except Exception as e:
                print(f"Error for {ticker}: {e}")

            finally:
                counter += 1
                if counter % 50 == 0:
                    print(f"Processed {counter} tickers")
                    time.sleep(2)

        # Merge with tickers_df and get TXT files
        merged_df = pd.merge(
            eod_df,
            self.tickers_df,
            left_on="ticker",
            right_on="esignal",
            how="left",
        )

        merged_df = merged_df[
            ~merged_df["date"].isin(self.undesired_dates)
        ].reset_index(drop=True)

        return merged_df

    def save_txt_files(self, merged_df):
        """
        Saving txt files for each exchange
        """
        exchange_dfs = []
        for exchange in self.exchanges_of_interest:
            if exchange == "NYSE ARCA":
                filename = self.output_path + "NYSE.AMEX.TXT"
            else:
                filename = self.output_path + f"{exchange}.TXT"

            exchange_df = self.exchange_df_to_txt(merged_df, exchange, filename)
            exchange_dfs.append(exchange_df)

        return exchange_dfs
