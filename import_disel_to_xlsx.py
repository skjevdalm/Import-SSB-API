import requests
import pandas as pd
from pyjstat import pyjstat
import logging
import sys
from datetime import datetime

# Konfigurer logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(url, json_data):
    """Henter data fra SSB API og returnerer JSON-stat data."""
    try:
        response = requests.post(url, json=json_data)
        response.raise_for_status()  # Hever en HTTPError hvis statuskoden er 4xx/5xx
        logging.info("Data hentet vellykket fra API-et.")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Forespørselen mislyktes: {e}")
        sys.exit(1)

def parse_data(json_stat):
    """Parser JSON-stat data og returnerer en Pandas DataFrame."""
    try:
        dataset = pyjstat.Dataset.read(json_stat)
        df = dataset.write('dataframe')
        logging.info("Data parsingen var vellykket.")
        return df
    except Exception as e:
        logging.error(f"Feil under parsing av data: {e}")
        sys.exit(1)

def split_date_column(df, date_column):
    """Splitter datoer fra YYYY*M*MM til to separate kolonner 'år' og 'måned'."""
    try:
        # Fjern 'M' fra datoene
        df[date_column] = df[date_column].str.replace('M', '')
        
        # Split kolonnen i to nye kolonner 'år' og 'måned'
        df[['år', 'måned']] = df[date_column].str.extract(r'(\d{4})(\d{2})')
        
        # Konverter til numeriske verdier
        df['år'] = pd.to_numeric(df['år'])
        df['måned'] = pd.to_numeric(df['måned'])
        
        logging.info("Datoformatet er oppdatert og splittet til 'år' og 'måned'.")
    except Exception as e:
        logging.error(f"Feil under splitting av datoer: {e}")
        sys.exit(1)

def save_to_excel(df, filename):
    """Lagrer Pandas DataFrame til en Excel-fil."""
    try:
        df.to_excel(filename, index=False)
        logging.info(f"Data lagret som {filename}.")
    except Exception as e:
        logging.error(f"Feil under lagring til Excel: {e}")
        sys.exit(1)

def main():
    url = "https://data.ssb.no/api/v0/no/table/09654/"
    json_data = {
        "query": [
            {
                "code": "PetroleumProd",
                "selection": {
                    "filter": "item",
                    "values": [
                        "035"
                    ]
                }
            }
        ],
        "response": {
            "format": "json-stat2"
        }
    }
    
    # Hent data fra API
    json_stat = fetch_data(url, json_data)
    
    # Parse data til DataFrame
    df = parse_data(json_stat)
    
    # Split datoer i kolonnen "måned" til to separate kolonner "år" og "måned"
    if 'måned' in df.columns:
        split_date_column(df, 'måned')
    else:
        logging.warning("Kolonnen 'måned' finnes ikke i dataene.")
    
    # Lagre DataFrame til Excel
    save_to_excel(df, "output.xlsx")

if __name__ == "__main__":
    main()
