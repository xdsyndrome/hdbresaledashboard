import os
import yaml
import requests
import logging
import pandas as pd
import datetime as dt
from datetime import date


class HDBDataset:
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
        dir_ = os.path.join(os.getcwd(), 'config.yaml')
        with open(fr'{dir_}', 'r') as file:
            CONFIG_ = yaml.safe_load(file)
        return CONFIG_
    
    def download(self):
        logging.info('Downloading dataset.')
        url = self.config['download_url']
        req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        req_json = req.json()
        return req_json
    
    def convert_to_df(self, req_json):
        df = pd.DataFrame.from_dict(req_json['result']['records'])            
        return df
    
    def parse_cols(self):
        self.parse_price()
        self.parse_floor_area()
        self.parse_psm()
        self.parse_month()
        self.parse_lease()
        self.parse_address()
        return
    
    def parse_lease(self):
        self.dataset['remaining_lease_years'] = 99 - (self.dataset['month'].dt.year - self.dataset['lease_commence_date'].astype(int))
        return
    
    def parse_price(self):
        self.dataset['resale_price'] = self.dataset['resale_price'].astype(float)
        return
    
    def parse_psm(self):
        self.dataset['psm'] = self.dataset['resale_price'] / self.dataset['floor_area_sqm']
        return
    
    def parse_floor_area(self):
        self.dataset['floor_area_sqm'] = self.dataset['floor_area_sqm'].astype(float)
        return
    
    def parse_month(self):
        self.dataset['month'] = pd.to_datetime(self.dataset['month'])
        return
    
    def parse_address(self):
        self.dataset['address'] = self.dataset['block'] + " " +  self.dataset['street_name']
        return
    
    def save_df(self):
        self.dataset.to_csv('data/hdbresale.csv')
        return
