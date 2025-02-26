from typing import Optional, Dict, List
import pandas as pd
import logging
from datetime import datetime, timedelta
import yfinance as yf
from sqlalchemy import create_engine
import os


class StockDataValidator:
    """"Handles all data validation logic"""
    def validate_stock_data(self, df: pd.DataFrame) -> tuple[bool, Dict]:
        checks = {
            'missing_data': df.isnull().sum().sum() == 0,
            'negative_prices': not (
                (df['open'] < 0) | 
                (df['high'] < 0) | 
                (df['low'] < 0) | 
                (df['close'] < 0)
            ).any(),
            'volume_check': not (df['volume'] < 0).any(),
            'high_low_check': not (df['high'] < df['low']).any()
        }
        return all(checks.values()), checks



class StockDataExtractor:
    """Handles data extraction from YFinance"""
    def __init__(self, validator: StockDataValidator):
        self.validator = validator
        self.logger = logging.getLogger(__name__)


    def _process_dataframe(self, ticker_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        ticker_df['symbol'] = symbol
        ticker_df = ticker_df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })

        columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        return ticker_df[columns]

    def fetch_stock_data(self, symbol: str, days_of_history: int = 2) -> Optional[pd.DataFrame]:
        try: 

            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_of_history)

            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date, interval='1d')
            df = df.reset_index()

            df = self._process_dataframe(df, symbol)

            is_valid, checks = self.validator.validate_stock_data(df)
            if not is_valid:
                self.logger.error(f"Data validation failed for {symbol}: {checks}")
                return None
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
            return None




class DatabaseHandler: 
    """"Handles database operations"""
    def __init__(self, connections_string: str):
        self.engine = create_engine(connections_string)
        self.logger = logging.getLogger(__name__)

    def save_to_db(self, df: pd.DataFrame) -> bool:
        try:
            df.to_sql(
                'daily_prices',
                self.engine,
                schema='stock_data',
                if_exists='replace',
                index=False
            )
            return True
        
        except Exception as e:
            self.logger(f"Error saving to database: {e}")
            return False


def main():
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Initialize components
    validator = StockDataValidator()
    extractor = StockDataExtractor(validator)
    DATABASE_URL = os.getenv("POSTGRESQL_STOCK_DB_URL")
    print(f"DATABASE_URL: {DATABASE_URL}")
    db_handler = DatabaseHandler(DATABASE_URL)
    
    # Define symbols
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    
    # Process each symbol
    for symbol in symbols:
        print("Extracting")
        df = extractor.fetch_stock_data(symbol)
        if df is not None:
            print("Saving")
            success = db_handler.save_to_db(df)
            if success:
                logging.info(f"Successfully processed {symbol}")


if __name__ == "__main__":
    main()
