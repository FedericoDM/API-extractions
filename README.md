# API Extractions

This repository exemplifies some work I have done regarding API usage and data extraction for several projects in the past.

### IEXCloud

The *iexcloud_data* folder contains a script that calls the IEXCloud API to fetch real-time OHLC data for a set of pre-defined tickers.

### EOD

The *eod_data* folder contains codes that extract adjusted end-of-day prices for a set of tickers. This data is then manipulated into a desirable format, uploaded to an S3 AWS bucket, and sent via e-mail.

### DOF

The *dof* folder contains a class that extracts the data from Mexico's *Official Journal of the Federation* by using their [public API](https://sidof.segob.gob.mx/datos_abiertos). The code extracts, for a given date, all of the diary's notes, the document itself and the JSON file with all of the information. This is then uploaded to the user's S3 Bucket in AWS. 
