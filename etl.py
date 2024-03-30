import pandas as pd

class Sales_Trend_Dataset_Processor:
    def __init__(self, daily_sales, interested_sales_trends, item_categories, items, shops):
        self.daily_sales = daily_sales
        self.interested_sales_trends = interested_sales_trends
        self.item_categories = item_categories
        self.items = items
        self.shops = shops
    
    def extract(self, filename):
        df = pd.read_csv(filename)
        return df
    
    def transform(self):
        daily_sales = self.extract(self.daily_sales)
        items = self.extract(self.items)
        item_categories = self.extract(self.item_categories)
        interested_sales_trends = self.extract(self.interested_sales_trends)

        # Handle missing values
        daily_sales['item_price'].fillna(daily_sales['item_price'].mean(), inplace=True)  # Replace missing item_price with mean

        # Correct data types
        daily_sales['date'] = pd.to_datetime(daily_sales['date'], format='%d.%m.%Y') 

        # Feature Engineering
        # 1. Calculate total revenue
        daily_sales['total_revenue'] = daily_sales['item_price'] * daily_sales['item_cnt_day']

        # 2. Extract month and year
        daily_sales['month'] = daily_sales['date'].dt.month
        daily_sales['year'] = daily_sales['date'].dt.year

        # Delete Empty columns
        del item_categories['item_category_name']
        del items['item_name']

        # Merge Datasets (if needed for analysis)
        sales_data = pd.merge(daily_sales, items, on='item_id', how='left')
        sales_data = pd.merge(sales_data, item_categories, on='item_category_id', how='left')

        # Filter for interested sales trends
        final_data = pd.merge(sales_data, interested_sales_trends, on=['shop_id', 'item_id'], how='inner')

        return final_data
    
    def load(self, df, filename):
        df.to_csv(filename)
        return

    def create_daily_sales_pivots(self, daily_sales):
        # 1.
        sales_pivot = pd.pivot_table(daily_sales, 
                             index=['shop_id', 'item_id'], 
                             columns=['month', 'year'],
                             values='item_cnt_day', 
                             aggfunc='sum',
                             fill_value=0)
        
        # 2.
        price_pivot = pd.pivot_table(daily_sales,
                             index='item_category_id',
                             values='item_price',
                             aggfunc='mean',
                             fill_value=0)
        
        # 3.
        shop_sales_pivot = pd.pivot_table(daily_sales,
                                  index='shop_id',
                                  columns='date_block_num',
                                  values='total_revenue',
                                  aggfunc='sum',
                                  fill_value=0)
        
        return sales_pivot, price_pivot, shop_sales_pivot
    
    def create_final_data_pivots(self, final_data):
        # 1.
        category_sales_pivot = pd.pivot_table(final_data,
                                        index='item_category_id',
                                        columns=['month', 'year'],
                                        values='total_revenue',
                                        aggfunc='sum',
                                        fill_value=0)
        
        # 2.
        top_items_pivot = pd.pivot_table(final_data,
                                 index=['item_id', 'item_category_id'],  # Multi-level index 
                                 columns='month', 
                                 values='item_cnt_day', 
                                 aggfunc='sum',
                                 fill_value=0)
        
        return category_sales_pivot, top_items_pivot
    
if __name__ == '__main__':
    DAILY_SALES = 'sales_trend_data/daily_sales.csv'
    INTERESTED_SALES_TRENDS = 'sales_trend_data/interested_sales_trends.csv'
    ITEM_CATEGORIES = 'sales_trend_data/item_categories.csv' 
    ITEMS = 'sales_trend_data/items.csv'
    SHOPS = 'sales_trend_data/shops.csv'

    etl = Sales_Trend_Dataset_Processor(DAILY_SALES, INTERESTED_SALES_TRENDS, ITEM_CATEGORIES, ITEMS, SHOPS)
    
    # Transforming and merging daily sales
    final_data = etl.transform()
   
    # Loading processed data
    etl.load(final_data, 'final_data.csv')

    # Creating pivots from daily_sales data only for simple analysis
    (sales_pivot, price_pivot, shop_sales_pivot) = etl.create_daily_sales_pivots(final_data)
    etl.load(sales_pivot, 'sales_pivot.csv')
    etl.load(price_pivot, 'price_pivot.csv')
    etl.load(shop_sales_pivot, 'shop_sales_pivot.csv')

    # # Creating pivots from final merged data only for detailed analysis
    (category_sales_pivot, top_items_pivot) = etl.create_final_data_pivots(final_data)
    etl.load(category_sales_pivot, 'category_sales_pivot.csv')
    etl.load(top_items_pivot, 'top_items_pivot.csv')
