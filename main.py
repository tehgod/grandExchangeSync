import requests
import os
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from time import sleep

load_dotenv()
discord_id = os.getenv('discord_id')

class ItemPrices:
    def __init__(self, id):
        self.id = id
        self.highPrice = None
        self.highPriceTime = None
        self.lowPrice = None
        self.lowPriceTime = None
        self.gePrice = None
        self.previousGePrice = None
            
    def to_dict(self):
        return {
            'id': self.id,
            'highPrice': self.highPrice,
            'highPriceTime': self.highPriceTime,
            'lowPrice': self.lowPrice,
            'lowPriceTime': self.lowPriceTime,
            'gePrice': self.gePrice,
            'previousGePrice': self.previousGePrice
        }

class ItemInfo:
    def __init__(self):
        self.id = id
        self.name = None
        self.examine = None
        self.members = None
        self.volume = None
        self.lowalch = None
        self.highalch = None
        self.buylimit = None
    
    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'examine': self.examine,
            'members': self.members,
            'volume': self.volume,
            'lowalch': self.lowalch,
            'highalch': self.highalch,
            'buylimit': self.buylimit
        }
    
#ingame prices
class OsrsWiki:
    def __init__(self):
        self.prices = []
        self.get_data()

    def get_data(self):
        url = "https://prices.runescape.wiki/api/v1/osrs/latest"
        headers = {
            'User-Agent': f'GrandExchangeLocalDataSync v1.0 DiscId {discord_id}'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()['data']
            prices = []
            for item_id in data:
                item = ItemPrices(int(item_id))
                item_stats = data[item_id]
                item.highPrice = item_stats['high']
                item.highPriceTime = datetime.fromtimestamp(item_stats['highTime']) if isinstance(item_stats
                    ['highTime'], (int, float)) else item_stats['highTime']
                item.lowPrice = item_stats['low']
                item.lowPriceTime = datetime.fromtimestamp(item_stats['lowTime']) if isinstance(item_stats
                    ['lowTime'], (int, float)) else item_stats['lowTime']
                prices.append(item)
            self.prices = prices
            return prices
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

#current prices
class GrandExchange:
    def __init__(self):
        self.prices = []
        self.get_data()

    def get_data(self):
        url = "https://chisel.weirdgloop.org/gazproj/gazbot/os_dump.json"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            prices = []
            details = []
            for item in data:
                item_data = data[item]
                if type(item_data) is not dict:
                    continue
                item_priceobj = ItemPrices(item_data['id'])
                try:
                    item_priceobj.gePrice = item_data['price'] 
                except KeyError:
                    item_priceobj.gePrice = None
                try: 
                    item_priceobj.previousGePrice = item_data['last']
                except KeyError:
                    item_priceobj.previousGePrice = None
                prices.append(item_priceobj)

                item_info_obj = ItemInfo()
                item_info_obj.id = item_data['id']
                try:
                    item_info_obj.name = item_data['name']
                except KeyError:
                    item_info_obj.name = None
                try:
                    item_info_obj.examine = item_data['examine']
                except KeyError:
                    item_info_obj.examine = None
                try:
                    item_info_obj.members = item_data['members']
                except KeyError:
                    item_info_obj.members = None
                try:
                    item_info_obj.volume = item_data['volume']
                except KeyError:
                    item_info_obj.volume = None
                try:
                    item_info_obj.lowalch = item_data['lowalch']
                except KeyError:
                    item_info_obj.lowalch = None
                try:
                    item_info_obj.highalch = item_data['highalch']
                except KeyError:
                    item_info_obj.highalch = None
                try:
                    item_info_obj.buylimit = item_data['limit']
                except KeyError:
                    item_info_obj.buylimit = None
                details.append(item_info_obj)
            self.prices = prices
            self.details = details

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

class database_connection:
    def __init__(self, database_name):
        self.database_name = database_name
        self.load_engine()
        pass

    def load_engine (self):
        db_username = os.getenv('db_username')
        db_password = os.getenv('db_password')
        db_hostname = os.getenv('db_hostname')
        self.engine = create_engine(f"mysql+pymysql://{db_username}:{db_password}@{db_hostname}/{self.database_name}")
        return True

    def df_to_db(self, df, table_name):
        self.load_engine()
        #merge into database on Id as primary matching column, replacing values with new values
        df.set_index('Id', inplace=True)
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=True)
        self.engine.dispose()
        return True

    @staticmethod
    def df_to_dict(df):
        return df.to_dict(orient='records')
    
    def load_members(self):
        self.load_engine()
        df = pd.read_sql("select Username from Usernames where TrackStats=1", self.engine)
        usernames = []
        for entry in df.values.tolist():
            usernames.append(entry[0])
        self.engine.dispose()
        return usernames
   
    def insert_current_prices(self, values):
        self.load_engine()
        query = """INSERT INTO ItemPrices (Id, InGameHighPrice, InGameHighPriceTime, InGameLowPrice, InGameLowPriceTime, GePrice, PreviousGePrice)
            VALUES (:id, :highPrice, :highPriceTime, :lowPrice, :lowPriceTime, :gePrice, :previousGePrice)
            ON DUPLICATE KEY UPDATE
                InGameHighPrice = VALUES(InGameHighPrice),
                InGameHighPriceTime = VALUES(InGameHighPriceTime),
                InGameLowPrice = VALUES(InGameLowPrice),
                InGameLowPriceTime = VALUES(InGameLowPriceTime),
                GePrice = VALUES(GePrice),
                PreviousGePrice = VALUES(PreviousGePrice)
            """
        
        with self.engine.connect() as connection:
            if isinstance(values, list):
                for val in values:
                    connection.execute(text(query), val)
            else:
                connection.execute(text(query), values)
            connection.commit()
        self.engine.dispose()
        return True

    def insert_current_details(self, values):
        self.load_engine()
        query = """INSERT INTO ItemDetails (Id, Name, Examine, Members, Volume, LowAlch, HighAlch, BuyLimit)
            VALUES (:id, :name, :examine, :members, :volume, :lowalch, :highalch, :buylimit)
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                Examine = VALUES(Examine),
                Members = VALUES(Members),
                Volume = VALUES(Volume),
                LowAlch = VALUES(LowAlch),
                HighAlch = VALUES(HighAlch),
                BuyLimit = VALUES(BuyLimit)
            """
        
        with self.engine.connect() as connection:
            connection.execute(text(query), values)
            connection.commit()
        self.engine.dispose()
        return True
load_dotenv()

def update_data():
    db = database_connection("GrandExchange")
    wiki = OsrsWiki()
    ge = GrandExchange()

    #merge the two data sets on item id
    for item in wiki.prices:
        matching_items = [g for g in ge.prices if g.id == item.id]
        if matching_items:
            ge_item = matching_items[0]
            item.gePrice = ge_item.gePrice
            item.previousGePrice = ge_item.previousGePrice
    db.insert_current_prices([item.to_dict() for item in wiki.prices])
    db.insert_current_details([item.to_dict() for item in ge.details])

if __name__ == "__main__":
    while True:
        update_data()
        print(f"Data updated at {datetime.now()}")
        sleep(300)