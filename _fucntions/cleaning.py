import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine


def extract_clean(filename:str, output_path:str):
    '''
    Extract  data from csv file and save it to a new csv file
    @param file_path: str, path to the csv file
    @param output_path: str, path to save the new csv file
    '''

    df = pd.read_csv(filename)
    df = clean_column_names(df)
    df = handle_duplicates(df)
    df = impute_missing(df)
    df = new_features(df)
    df.to_parquet(output_path)

 
def extract_states(filename:str, output_path:str):
    '''
    Extract sales data from csv file and save it to a new csv file
    @param file_path: str, path to the csv file
    @param output_path: str, path to save the new parquet file
    '''
    df = pd.read_csv(filename)
    df = df.rename(columns={
    "code": "state"
    })
    df.to_parquet(output_path)

def combine_sources(filename1:str,filename2:str , output_path:str):
    '''
    Extract sales data from csv file and save it to a new csv file
    @param file_path: str, path to the csv file
    @param output_path: str, path to save the new parquet file
    '''
    
    
    fintech_clean = pd.read_parquet(filename1)
    # Load the states dataset
    fintech_states = pd.read_parquet(filename2)
    # Combine the two datasets (assuming both have a 'state' column to join on)
    combined = pd.merge(fintech_clean, fintech_states, on="state", how="left")
    # Save the combined dataset to a new Parquet file
    combined.to_parquet(output_path)

def encoding(filename:str, output_path:str):
    '''
    Transform data by imputing missing values and encoding categorical columns
    @param filename: str, path to the csv file
    @param output_path: str, path to save the new csv file
    '''

    df = pd.read_parquet(filename)
    df = normalize_data(df)
    df = data_encoding(df)
    df.to_parquet(output_path)

def load_to_db(filename:str, table_name:str, postgres_opt:dict):
    '''
    Load the transformed data to the database
    @param filename: str, path to the csv file
    @param table_name: str, name of the table to create
    @param postgres_opt: dict, dictionary containing postgres connection options (user, password, host,port, db)
    '''
    user, password, host, port, db = postgres_opt.values()
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_parquet(filename)
    # Set the index to customer_id
    df.set_index('customer_id', inplace=True)
    df.to_sql(table_name, con=engine, if_exists='replace', index=True, index_label='customer_id')

# ---- Helper Functions ----

def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    return df

# def calculate_tax(price, quantity):
  #  return (price * quantity) * 0.05

def handle_duplicates(df):
    #deal with inconsistent string in loan type
    df['type'] = df['type'].replace({'INDIVIDUAL': 'Individual', 'JOINT': 'Joint App', 'DIRECT_PAY': 'Direct_pay'})
    
    return df

def impute_missing(df):
    # Impute missing emp_title and length
    df['emp_title'].fillna("unknown", inplace=True)
    df['emp_length'].fillna("unknown", inplace=True)

    # Impute missing tax 5%
    df['description'].fillna(df['purpose'], inplace=True)

    #impute interest rate
    median_int_rate_by_grade = df.groupby('grade')['int_rate'].median()
    df['int_rate'] = df.apply(lambda row: median_int_rate_by_grade[row['grade']] if pd.isnull(row['int_rate']) else row['int_rate'], axis=1)

    #impute income joint
    df['annual_inc_joint'].fillna(0, inplace=True)
    return df

def new_features(df):
    #month number
    df['issue_date'] = pd.to_datetime(df['issue_date'], format='%d %B %Y')
    df['month_number'] = df['issue_date'].dt.month

    #salary_cover
    df['salary_cover'] = (df['annual_inc'] >= df['loan_amount']).astype(int)

    ##monthly installment
    df['monthly_interest_rate'] = df['int_rate'] / 12

    # Determine the number of months (n) based on term column
    df['n'] = df['term'].str.extract('(\d+)').astype(int)

    # Calculate the numerator and denominator of the formula
    df['numerator'] = df['monthly_interest_rate'] * (1 + df['monthly_interest_rate']) ** df['n']
    df['denominator'] = (1 + df['monthly_interest_rate']) ** df['n'] - 1

    # Calculate monthly installment (M)
    df['monthly_installment'] = df['loan_amount'] * (df['numerator'] / df['denominator'])

    df.drop(columns=['monthly_interest_rate', 'numerator', 'denominator', 'n'], inplace=True)
    
    #letter grade
    bins = [0, 5, 10, 15, 20, 25, 30, 35]  
    labels = ['A', 'B', 'C', 'D', 'E', 'F', 'G']  

    df['letter_grade'] = pd.cut(df['grade'], bins=bins, labels=labels, right=True)

    return df

def data_encoding(df):
    global lookup
    lookup = pd.DataFrame(columns=['Column', 'Old Value', 'New Value'])
    df['pymnt_plan'] = df['pymnt_plan'].replace({'true': 1, 'false': 0})

    # Label Encoding
    label_encoders = {}
    columns_to_encode = ['home_ownership', 'verification_status', 'addr_state', 'purpose', 'term', 'type', 'loan_status']

    for column in columns_to_encode:
        le = LabelEncoder()
        df[column] = le.fit_transform(df[column])
        label_encoders[column] = le
        # Save the mapping
        mapping_df = pd.DataFrame({
            'Column': [column] * len(le.classes_),
            'Old Value': le.classes_,
            'New Value': range(len(le.classes_))
        })
        lookup = pd.concat([lookup, mapping_df], ignore_index=True)

    return df
      
def normalize_data(df):

    from sklearn.preprocessing import MinMaxScaler

    features_to_normalize = ['annual_inc', 'annual_inc_joint', 'loan_amount', 'int_rate', 'avg_cur_bal', 'tot_cur_bal', 'monthly_installment']

    scaler = MinMaxScaler()

    df[features_to_normalize] = scaler.fit_transform(df[features_to_normalize])
    return df