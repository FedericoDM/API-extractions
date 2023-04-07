# API Extractions

This repository exemplifies some work I have done regarding API usage and data extraction for several projects in the past.

### IEXCloud

The *iexcloud_data* folder contains a script which calls the IEXCloud API to fetch real-time OHLC data for a set of pre-defined tickers.

### EOD

The *eod_data* folder contains a series of codes which extract adjusted end of day prices for a set of tickers. This data is then manipulated into a desirable format, is uploaded to an S3 AWS bucket and then is sent via e-mail.
