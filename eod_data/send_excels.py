import pytz
import requests
import sendgrid
import pandas as pd
from datetime import datetime
from sendgrid.helpers.mail import Content, Mail


# Local imports
from keys import eod_keys, email_keys, recipients, bucket_name
from EODExtractor import EODExtractor
from file_uploading import upload_to_s3


# PARAMETERS
LOCAL_PATH = (
    "C:/Users/fdmol/Desktop/Interstellar_mte/interstellar-mte/src/eod_process/data/"
)
holidays_api = f"https://eodhistoricaldata.com/api/exchange-details/US?api_token={eod_keys['token']}&fmt=json&from=2000-01-01"
urls_dict = {}
exchanges_list = []

# FUNCTIONS

# Get all dates from US JSON holidays


def get_us_holidays_dates(holidays_api):
    """
    Get all US holidays dates from JSON

    Parameters
    ----------
    holidays_api: str
        API to get US holidays

    Returns
    -------
    us_holidays_dates: list
        List of US holidays dates
    """

    print("Getting US holidays dates...")
    us_holidays = requests.get(holidays_api.replace("EXCHANGE_CODE", "NASDAQ")).json()
    us_holidays_dates = []
    keys = us_holidays["ExchangeHolidays"]
    for key in keys:
        sub_dict = us_holidays["ExchangeHolidays"][key]
        date = sub_dict["Date"]
        us_holidays_dates.append(date)
    return us_holidays_dates


# Email function
def send_email_html(recipient, alert_subject, html):
    """
    Manda la alerta correspondiente:
        -recipient: string con el correo del recipiente
        -alert_subject: string con el subject del correo
        -html: body del correo con la info de la noticia
    """
    api_key = email_keys["api_key"]
    sg = sendgrid.SendGridAPIClient(api_key)

    from_email = "fd.molina@outlook.com"
    to_email = recipient
    subject = alert_subject

    # Add Intro
    html_text = "<html>" + html + "</html>"
    content = Content("text/html", html)

    mail = Mail(from_email, to_email, subject, content)
    sg.send(mail)

    print(f"email sent: {recipient}")


us_holidays_dates = get_us_holidays_dates(holidays_api)
eod_extractor = EODExtractor(
    LOCAL_PATH + "tickers_to_use(3).csv",
    "",
)
merged_df = eod_extractor.get_eod_data()
# merged_df = pd.read_csv(LOCAL_PATH + "eod_adjusted_historical.csv", encoding="utf-8")

# First, group the merged_df by exchange
grouped = merged_df.groupby("Exchange")
rare_tickers = ["SVXY", "USMV", "UVXY", "VIXM", "VIXY", "VXX", "VXZ"]
merged_df.loc[merged_df["ticker"].isin(rare_tickers), "Exchange"] = "NYSE"

# Place BATS and NYSE MKT in NYSE
merged_df.loc[merged_df["Exchange"] == "BATS", "Exchange"] = "NYSE"
merged_df.loc[merged_df["Exchange"] == "NYSE MKT", "Exchange"] = "NYSE"

# Today
today = datetime.now(pytz.timezone("America/Mexico_city")).strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)

# Iterate through each group (exchange) and create an Excel file
for exchange, group_df in grouped:
    # Create a Pandas Excel writer object for the current exchange
    filename = f"{exchange}.xlsx"
    writer = pd.ExcelWriter(f"/tmp/{exchange}.xlsx", engine="xlsxwriter")

    # Iterate through each ticker in the current exchange group
    for ticker in group_df["ticker"].unique():
        # Filter the group dataframe to only include the current ticker
        ticker_df = group_df[group_df["ticker"] == ticker]

        # Write the ticker dataframe to a new sheet in the Excel file
        sheet_name = ticker.replace(
            "/", "_"
        )  # Replace any forward slashes in the ticker name with underscores
        ticker_df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save and close the Excel writer object
    writer.save()
    upload_to_s3(filename, "finviz-datos", "eod_data")
    print(f"Saved catalogue for {today}!")
    print(f"Finished writing {exchange} data to Excel file")
    exchanges_list.append(exchange)

    todays_key = f"eod_data/{filename}"
    url_today = f"https://{bucket_name}.s3.us-west-1.amazonaws.com/{todays_key}"
    urls_dict[exchange] = url_today

# Configurar subject y html
subject = f"Datos End of Day - {today[:10]}"

html = f"""
Hola,
<p>
Abajo, encontrarás los Excel correspondientes al EOD de hoy, ({today[:10]}).
</p>
<p>
{exchanges_list[0]}:
<a href={urls_dict[exchanges_list[0]]}>{exchanges_list[0]}_{today[:10]}</a> 
</p>
<p>
{exchanges_list[1]}:
<a href={urls_dict[exchanges_list[1]]}>{exchanges_list[1]}_{today[:10]}</a> 
</p>
<p>
{exchanges_list[2]}:
<a href={urls_dict[exchanges_list[2]]}>{exchanges_list[2]}_{today[:10]}</a> 
</p>
<p>
Que tengas lindo día.
</p>"""

for recipient in recipients:
    send_email_html(recipient, subject, html)
