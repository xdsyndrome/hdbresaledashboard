import pandas as pd
import requests
import json
from tqdm import tqdm
from geopy.distance import geodesic
from Download import HDBDataset

class MRTDataset:
    def __init__(self):
        self.mrt_data = self.read_mrt()
    
    def read_mrt(self):
        mrt_data = pd.read_csv('data/MRT Stations.csv')
        mrt_data["mrt"] = mrt_data["STN_NAME"].apply(MRTDataset.parse_mrt_name)
        return mrt_data
    
    @staticmethod
    def parse_mrt_name(stn):
        return stn[:-12].lower()


class HDBGeo(HDBDataset, MRTDataset):
    def __init__(self, download):
        super().__init__(download)
        super(HDBDataset, self).__init__()
        if download:
            self.dataset_geo = self.get_hdb_geo()
            self.dataset_merged = self.merge_mrt()
            assert len(self.dataset_geo) == len(self.dataset_merged)
            self.dataset_merged.to_csv('data/dataset_merged.csv')
        else:
            try:
                print('no downloading')
                self.dataset_merged = pd.read_csv('data/dataset_merged.csv')
            except OSError as e:
                print(e.errno)
    
    def get_hdb_geo(self):
        geo_data = self.get_geo_info()
        df_output = self.dataset.merge(geo_data,
                                       how='left',
                                       on='address')
        return df_output
    
    def get_unique_address(self):
        return self.dataset['address'].unique()
        
    def get_geo_info(self):
        latitude = []
        longitude = []
        postal_code = []
        address_list = []
        for address in tqdm(self.get_unique_address()):
            query_string='https://developers.onemap.sg/commonapi/search?searchVal='+str(address)+'&returnGeom=Y&getAddrDetails=Y&pageNum=1'
            resp=requests.get(query_string)
            # Convert json object into Python dict
            data=json.loads(resp.content)
            address_list.append(address)
            # Get first search result
            if data["found"]:
                latitude.append(data["results"][0]["LATITUDE"])
                longitude.append(data["results"][0]["LONGITUDE"])
                postal_code.append(data["results"][0]["POSTAL"])

            # If no search result, set as None
            else:
                latitude.append(None)
                longitude.append(None)
                postal_code.append(None)
        
        df_output = pd.DataFrame([address_list, latitude, longitude, postal_code]).transpose()
        df_output.columns = ['address', 'latitude', 'longitude', 'postal_code']
        return df_output
    
    def get_nearest_mrt(self):
        # Prepare List of HDB Coordinates and MRT Coordinates 
        hdb_postal = self.dataset_geo["postal_code"]
        hdb_lat = self.dataset_geo["latitude"]
        hdb_long = self.dataset_geo["longitude"]

        mrt_stn = list(zip(self.mrt_data["mrt"], self.mrt_data["STN_NO"]))
        mrt_lat = self.mrt_data["Latitude"]
        mrt_long = self.mrt_data["Longitude"]

        hdb_coord = list(zip(hdb_postal, hdb_lat, hdb_long))
        mrt_coord = list(zip(mrt_lat, mrt_long))
            # List containing the minimum distance for each HDB block
        list_of_nearest_distance = []
        
        for hdb in tqdm(hdb_coord):
            # Need to create a cache for this to prevent having to check each time
            # save as json in cache, then load as dict 
            list_of_mrt_distance_to_each_hdb = []
            for mrt in mrt_coord:
                list_of_mrt_distance_to_each_hdb.append(geodesic((hdb[1], hdb[2]), mrt).meters)
            nearest = min(list_of_mrt_distance_to_each_hdb)    
            list_of_nearest_distance.append((str(hdb[0]), mrt_stn[list_of_mrt_distance_to_each_hdb.index(nearest)], nearest))
        
        nearest_mrt = []
        for i in list_of_nearest_distance:
            nearest_mrt.append((i[0], i[1][0], i[1][1], i[2]))
        geo_distance = pd.DataFrame(nearest_mrt, columns=["postal_code", "mrt", "stn_no", "distance_meters"])
        geo_distance = geo_distance.drop_duplicates()
        return geo_distance
    
    def merge_mrt(self):
        geo_distance = self.get_nearest_mrt()
        output = self.dataset_geo.merge(geo_distance,
                                        how='left',
                                        on='postal_code')
        return output
      
