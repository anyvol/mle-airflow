#dags/clean_churn.py

import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def clean_churn_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    @task()
    def create_table():
        from sqlalchemy import inspect, MetaData, Table, Column, String, Integer, DateTime, Float, UniqueConstraint
        metadata = MetaData()
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        clean_users_churn = Table(
            'clean_users_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date', DateTime),
            Column('type', String),
            Column('paperless_billing', String),
            Column('payment_method', String),
            Column('monthly_charges', Float),
            Column('total_charges', Float),
            Column('internet_service', String),
            Column('online_security', String),
            Column('online_backup', String),
            Column('device_protection', String),
            Column('tech_support', String),
            Column('streaming_tv', String),
            Column('streaming_movies', String),
            Column('gender', String),
            Column('senior_citizen', Integer),
            Column('partner', String),
            Column('dependents', String),
            Column('multiple_lines', String),
            Column('target', Integer),
            UniqueConstraint('customer_id', name='unique_clean_customer_constraint')
        )
        if not inspect(engine).has_table('clean_users_churn'):
            metadata.create_all(engine, tables=[clean_users_churn])

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = f"""
        select *
        from users_churn
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        def remove_duplicates(data):
            feature_cols = data.columns.drop('customer_id').tolist()
            is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data
        
        def fill_missing_values(data):
            cols_with_nans = data.isnull().sum()
            cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')
            for col in cols_with_nans:
                if data[col].dtype in [float, int]:
                    fill_value = data[col].mean()
                elif data[col].dtype == 'object':
                    fill_value = data[col].mode().iloc[0]
            data[col] = data[col].fillna(fill_value)
            return data

        def remove_outliers(data):
            num_cols = data.select_dtypes(['float']).columns
            threshold = 1.5
            potential_outliers = pd.DataFrame()

            for col in num_cols:
                Q1 = data[col].quantile(0.25)  # Calculate the first quartile
                Q3 = data[col].quantile(0.75)  # Calculate the third quartile
                IQR = Q3 - Q1  # Calculate the Interquartile Range
                margin = threshold * IQR  # Calculate the margin for outlier detection
                lower = Q1 - margin  # Calculate the lower bound for outliers
                upper = Q3 + margin  # Calculate the upper bound for outliers
                potential_outliers[col] = ~data[col].between(lower, upper)  # Identify potential outliers
                print(f'potential_outliers in {col}, number: {sum(potential_outliers[col])},IQR={IQR}, margin={margin}')

            outliers = potential_outliers.any(axis=1)  # Combine potential outliers across columns
            data = data[~outliers]
            return data
        
        return remove_outliers(fill_missing_values(remove_duplicates(data)))

    @task()
    # Function to load data into a PostgreSQL table
    def load(data: pd.DataFrame):
        # Create a PostgresHook object connected to the 'destination_db' database
        hook = PostgresHook('destination_db')
        
        # Convert the 'end_date' column to an object type and replace NaN values with None
        data['end_date'] = data['end_date'].astype('object').replace(np.nan, None)
        
        # Insert rows into the 'clean_users_churn' table in the database
        hook.insert_rows(
            # Specify the target table
            table="clean_users_churn",
            
            # Set replace mode to True, overwriting any existing data
            replace=True,
            
            # Define the column order for the insert operation (i.e., target_fields)
            target_fields=data.columns.tolist(),
            
            # Specify the index field(s) to match with the existing table (in this case, 'customer_id')
            replace_index=['customer_id'],
            
            # Convert the DataFrame to a list of lists and pass it as the rows parameter
            rows=data.values.tolist()
        )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_churn_dataset()