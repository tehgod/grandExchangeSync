import requests
import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv
from time import sleep
from urllib.parse import quote_plus

load_dotenv()
discord_id = os.getenv('discord_id')

class DatabaseConnection:
    def __init__(self, database_name):
        self.database_name = database_name
        db_username = os.getenv('db_username')
        db_password = os.getenv('db_password')
        db_hostname = os.getenv('db_hostname')
        connection_string = f"mssql+pyodbc://{db_username}:{quote_plus(db_password)}@{db_hostname}/Runescape?driver=ODBC Driver 17 for SQL Server"
        # Create engine once with connection pool settings
        self.engine = create_engine(
            connection_string,
            pool_size=5,          # Keep 5 connections in the pool
            max_overflow=10,      # Allow up to 10 extra connections
            pool_pre_ping=True,   # Verify connections before using
            pool_recycle=3600,    # Recycle connections after 1 hour
            fast_executemany=True # Enable fast batch inserts for pyodbc
        )
    
    def close(self):
        self.engine.dispose()

    def df_to_db(self, df, table_name):
        #merge into database on Id as primary matching column, replacing values with new values
        df.set_index('Id', inplace=True)
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=True)
        return True

class ItemPrice:
    def __init__(self, itemId):
        self.itemId = itemId
        self.inGameHighPrice = None
        self.inGameHighPriceTimestamp = None
        self.inGameLowPrice = None
        self.inGameLowPriceTimestamp = None
        self.gePrice = None
        self.previousGePrice = None
            
    def to_dict(self):
        return {
            'itemId': self.itemId,
            'inGameHighPrice': self.inGameHighPrice,
            'inGameHighPriceTimestamp': self.inGameHighPriceTimestamp,
            'inGameLowPrice': self.inGameLowPrice,
            'inGameLowPriceTimestamp': self.inGameLowPriceTimestamp,
            'gePrice': self.gePrice,
            'previousGePrice': self.previousGePrice
        }

class ItemDetail:
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

class PricesApiItem:
    def __init__(self, itemId, itemInfo):
        self.itemId = itemId
        self.inGameHighPrice = itemInfo.get('high')
        highTime = itemInfo.get('highTime')
        self.inGameHighPriceTimestamp = (datetime.fromtimestamp(highTime) 
                                         if highTime and isinstance(highTime, (int, float)) 
                                         else None)
        self.inGameLowPrice = itemInfo.get('low')
        lowTime = itemInfo.get('lowTime')
        self.inGameLowPriceTimestamp = (datetime.fromtimestamp(lowTime) 
                                       if lowTime and isinstance(lowTime, (int, float)) 
                                       else None)
    
    def to_dict(self):
        return {
            'itemId': self.itemId,
            'inGameHighPrice': self.inGameHighPrice,
            'inGameHighPriceTimestamp': self.inGameHighPriceTimestamp,
            'inGameLowPrice': self.inGameLowPrice,
            'inGameLowPriceTimestamp': self.inGameLowPriceTimestamp
        }

class WierdGloopItem:
    def __init__(self, itemId, itemData):
        self.itemId = itemId
        self.examine = itemData.get('examine')
        self.members = itemData.get('members')
        self.lowAlch = itemData.get('lowalch')
        self.highAlch = itemData.get('highalch')
        self.buyLimit = itemData.get('limit')
        self.name = itemData.get('name')
        self.previousGePrice = itemData.get('last')
        self.gePrice = itemData.get('price')
        self.volume = itemData.get('volume')
    
    def to_dict_details(self):
        return {
            'itemId': self.itemId,
            'name': self.name,
            'examine': self.examine,
            'members': self.members,
            'volume': self.volume,
            'lowalch': self.lowAlch,
            'highalch': self.highAlch,
            'buylimit': self.buyLimit
        }
    
    def to_dict_prices(self):
        return {
            'itemId': self.itemId,
            'gePrice': self.gePrice,
            'previousGePrice': self.previousGePrice
        }

class OsrsWiki:
    def __init__(self):
        self.items: list[PricesApiItem] = []
        self.get_data()

    def get_data(self):
        url = "https://prices.runescape.wiki/api/v1/osrs/latest"
        headers = {
            'User-Agent': f'GrandExchangeLocalDataSync v1.0 DiscId {discord_id}'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()['data']
        items = []
        for item_id in data:
            item = PricesApiItem(int(item_id), data[item_id])
            items.append(item)
        self.items = items
        return items

    def sync_data(self, db_conn: DatabaseConnection):
        query ="""MERGE INTO [GrandExchange].[ItemPrice] AS [p1]
                    USING
                    (
                        SELECT :itemId AS [ItemId],
                            :inGameHighPrice AS [InGameHighPrice],
                            :inGameHighPriceTimestamp AS [InGameHighPriceTimestamp],
                            :inGameLowPrice AS [InGameLowPrice],
                            :inGameLowPriceTimestamp AS [InGameLowPriceTimestamp]
                    ) AS [p2]
                    ON [p1].[ItemId] = [p2].[ItemId]
                    WHEN MATCHED AND (
                                        [p1].[InGameHighPrice] <> [p2].[InGameHighPrice]
                                        OR ISNULL([p1].[InGameHighPriceTimestamp], CAST('2026-01-01 12:30:00' AS DATETIME2)) <> ISNULL(
                                                                                                                                        [p2].[InGameHighPriceTimestamp],
                                                                                                                                        CAST('2026-01-01 12:30:00' AS DATETIME2)
                                                                                                                                    )
                                        OR [p1].[InGameLowPrice] <> [p2].[InGameLowPrice]
                                        OR ISNULL([p1].[InGameLowPriceTimestamp], CAST('2026-01-01 12:30:00' AS DATETIME2)) <> ISNULL(
                                                                                                                                        [p2].[InGameLowPriceTimestamp],
                                                                                                                                        CAST('2026-01-01 12:30:00' AS DATETIME2)
                                                                                                                                    )
                                    ) THEN
                        UPDATE SET [p1].[InGameHighPrice] = [p2].[InGameHighPrice],
                                [p1].[InGameHighPriceTimestamp] = [p2].[InGameHighPriceTimestamp],
                                [p1].[InGameLowPrice] = [p2].[InGameLowPrice],
                                [p1].[InGameLowPriceTimestamp] = [p2].[InGameLowPriceTimestamp]
                    WHEN NOT MATCHED THEN
                        INSERT
                        (
                            [ItemId],
                            [InGameHighPrice],
                            [InGameHighPriceTimestamp],
                            [InGameLowPrice],
                            [InGameLowPriceTimestamp]
                        )
                        VALUES
                        ([p2].[ItemId], [p2].[InGameHighPrice], [p2].[InGameHighPriceTimestamp], [p2].[InGameLowPrice],
                        [p2].[InGameLowPriceTimestamp]);"""

        values = []
        for item in self.items:
            values.append(item.to_dict())

        with db_conn.engine.connect() as connection:
            connection.execute(text(query), values)
            connection.commit()

        return True

class GrandExchange:
    def __init__(self):
        self.items: list[WierdGloopItem] = []
        self.get_data()

    def get_data(self):
        url = "https://chisel.weirdgloop.org/gazproj/gazbot/os_dump.json"
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        items = []
        for item_id in data:
            if not item_id.isdigit():
                continue
            item = WierdGloopItem(int(item_id), data[item_id])
            items.append(item)
        self.items = items

    def sync_data(self, db_conn: DatabaseConnection):
        query1 =         """MERGE INTO [GrandExchange].[ItemPrice] AS [t]
                        USING
                        (
                            SELECT :itemId AS [ItemId],
                                :gePrice AS [GePrice],
                                :previousGePrice AS [PreviousGePrice]
                        ) AS [s]
                        ON [s].[ItemId] = [t].[ItemId]
                        WHEN MATCHED THEN
                            UPDATE SET [t].[GePrice] = [s].[GePrice],
                                    [t].[PreviousGePrice] = [s].[PreviousGePrice]
                        WHEN NOT MATCHED THEN
                            INSERT
                            (
                                [ItemId],
                                [GePrice],
                                [PreviousGePrice]
                            )
                            VALUES
                            ([s].[ItemId], [s].[GePrice], [s].[PreviousGePrice]);"""
        values1 = []
        for item in self.items:
            values1.append(item.to_dict_prices())

        query2 ="""MERGE INTO [GrandExchange].[ItemDetail] AS [t]
                USING
                (
                    SELECT :itemId AS [ItemId],
                        :name AS [Name],
                        :examine AS [Examine],
                        :members AS [Members],
                        :volume AS [Volume],
                        :lowalch AS [LowAlch],
                        :highalch AS [HighAlch],
                        :buylimit AS [BuyLimit]
                ) AS [s]
                ON [s].[ItemId] = [t].[ItemId]
                WHEN MATCHED THEN
                    UPDATE SET [t].[Name] = [s].[Name],
                            [t].[Examine] = [s].[Examine],
                            [t].[Members] = [s].[Members],
                            [t].[Volume] = [s].[Volume],
                            [t].[LowAlch] = [s].[LowAlch],
                            [t].[HighAlch] = [s].[HighAlch],
                            [t].[BuyLimit] = [s].[BuyLimit]
                WHEN NOT MATCHED THEN
                    INSERT
                    (
                        [ItemId],
                        [Name],
                        [Examine],
                        [Members],
                        [Volume],
                        [LowAlch],
                        [HighAlch],
                        [BuyLimit]
                    )
                    VALUES
                    ([s].[ItemId], [s].[Name], [s].[Examine], [s].[Members], [s].[Volume], [s].[LowAlch], [s].[HighAlch],
                    [s].[BuyLimit]);"""

        values2 = []
        for item in self.items:
            values2.append(item.to_dict_details())

        with db_conn.engine.connect() as connection:
            connection.execute(text(query2), values2)
            connection.commit()
            connection.execute(text(query1), values1)
            connection.commit()
            
        
        return True

def main():
    try:
        osrs_wiki = OsrsWiki()
    except Exception as e:
        print(f"Error fetching data from OSRS Wiki: {e}")
        return False
    try:
        grand_exchange = GrandExchange()
    except Exception as e:
        print(f"Error fetching data from Grand Exchange: {e}")
        return False
    try:
        db_conn = DatabaseConnection('Runescape')
    except Exception as e:
        print(f"Error fetching data from Database Connection: {e}")
        return False
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if grand_exchange.sync_data(db_conn)==True:
        print(f"{timestamp} - Grand Exchange data synced successfully.")

    if osrs_wiki.sync_data(db_conn)==True: 
        print(f"{timestamp} - OSRS Wiki data synced successfully.")

if __name__ == "__main__":    
    while True:
        main()
        sleep(300)  # Sleep for 5 minutes