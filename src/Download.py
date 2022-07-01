import os
import yaml
import requests
import logging
import pandas as pd
import datetime as dt
from datetime import date


class HDBDataset:
    """Generate HDB dataset
    
    Attributes:
        self.run_date   date of running script
        self.config     loaded config file
        self.dataset    pd.DataFrame containing resale transactions
    """
    def __init__(self, download=False):
        FORMAT = '%(asctime)s:%(levelname)s:%(message)s'
        logging.basicConfig(format=FORMAT, level=logging.INFO)
        
        if download:
            self.run_date = date.today()
            self.config = self.get_config()
            
            self.dataset = self.convert_to_df(self.download())
            if self.dataset.empty:
                logging.info('No data downloaded.')
                return
            logging.info(f'Downloaded {len(self.dataset)} records.')
            self.parse_cols()
            self.save_df()
            
        else:
            self.dataset = pd.read_csv('data/hdbresale.csv')
            if self.dataset.empty:
                logging.info('No data loaded from directory.')
            else:
                logging.info(f'Successfully loaded {len(self.dataset)} records from directory.')
            
    def get_config(self):
        """Load config file

        Returns:
            _type_: _description_
        """
        dir_ = os.path.join(os.getcwd(), 'config.yaml')
        with open(fr'{dir_}', 'r') as file:
            CONFIG_ = yaml.safe_load(file)
        return CONFIG_
    
    def download(self):
        """downloads dataset from URL set in config

        Returns:
            _type_: _description_
        """
        logging.info('Downloading dataset.')
        url = self.config['download_url']
        req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        req_json = req.json()
        return req_json
    
    def convert_to_df(self, req_json) -> pd.DataFrame:
        """Converts json result into dataframe

        Args:
            req_json (_type_): json return

        Returns:
            pd.DataFrame: resale price dataset
        """
        df = pd.DataFrame.from_dict(req_json['result']['records'])            
        return df
    
    def parse_cols(self) -> None:
        """Wrapper function for parsing columns
        """
        self.parse_price()
        self.parse_floor_area()
        self.parse_psm()
        self.parse_month()
        self.parse_lease()
        self.parse_address()
        return
    
    def parse_lease(self) -> None:
        """Calculates remaining lease for flat
        """
        self.dataset['remaining_lease_years'] = 99 - (self.dataset['month'].dt.year - self.dataset['lease_commence_date'].astype(int))
        return
    
    def parse_price(self) -> None:
        """Cast price to float type
        """
        self.dataset['resale_price'] = self.dataset['resale_price'].astype(float)
        return
    
    def parse_psm(self) -> None:
        """Calculates price per sq meter
        """
        self.dataset['psm'] = self.dataset['resale_price'] / self.dataset['floor_area_sqm']
        return
    
    def parse_floor_area(self) -> None:
        """Cast floor area to float
        """
        self.dataset['floor_area_sqm'] = self.dataset['floor_area_sqm'].astype(float)
        return
    
    def parse_month(self) -> None:
        """Cast month to datetime object
        """
        self.dataset['month'] = pd.to_datetime(self.dataset['month'])
        return
    
    def parse_address(self) -> None:
        """Create address column from block and street_name
        """
        self.dataset['address'] = self.dataset['block'] + " " +  self.dataset['street_name']
        return
    
    def save_df(self) -> None:
        """Save file
        """
        #TO-DO: Set file location in global config
        self.dataset.to_csv('data/hdbresale.csv')
        return
