import pandas as pd
import matplotlib.pyplot as plt
from prefect import task,flow,get_run_logger
import os


@task
def fetcher(filepath): 
    logger=get_run_logger()
    # Step 1: Fetch Data
    print("Reading data...")
    # Assume a dataset with sales figures and other fields is provided.
    df = pd.read_csv(filepath)
    logger.info(f"Data shape: {df.shape}")
    #print(f"Data shape: {df.shape}")
    return df

@task
def validator(df:pd.DataFrame):
    logger=get_run_logger()
    
    # Step 2: Validate Data
    #print("Validating data...")
    missing_values = df.isnull().sum()
    #print("Missing values:\n", missing_values)
    logger.info("Missing values:\n", missing_values)
    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean

@task
def transformer(df_clean:pd.DataFrame):
    logger=get_run_logger()
    # Step 3: Transform Data
    print("Transforming data...")
    # For example, if there is a "sales" column, create a normalized version.
    if "sales" in df_clean.columns:
        df_clean["sales_normalized"] = (df_clean["sales"] - df_clean["sales"].mean()) / df_clean["sales"].std()

    logger.info("sales data normalized")
    return df_clean
    
@task
def analytics(df_clean:pd.DataFrame):
    logger=get_run_logger()
    # Step 4: Generate Analytics Report
    print("Generating analytics report...")
    summary = df_clean.describe()
    summary.to_csv("analytics_summary.csv")
    print("Summary statistics saved to data/analytics_summary.csv")
    logger.info("Summary statistics saved to data/analytics_summary.csv")

@task
def histogram_creator(df_clean:pd.DataFrame):
    logger=get_run_logger()
    # Step 5: Create a Histogram for Sales Distribution
    if "sales" in df_clean.columns:
        plt.hist(df_clean["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        print("Sales histogram saved to data/sales_histogram.png")
        logger.info("Sales histogram saved to data/sales_histogram.png")

    print("Analytics pipeline completed.")


@flow 
def analytics_pipeline():

    df=fetcher(r"C:\Users\Umar Farook\Documents\LUMS\Spring 2025\DE\Lab 6\analytics_data.csv")
    df_clean=validator(df)
    df_transformed=transformer(df_clean)
    analytics(df_transformed)
    histogram_creator(df_transformed)

if __name__ == "__main__":
    analytics_pipeline()
