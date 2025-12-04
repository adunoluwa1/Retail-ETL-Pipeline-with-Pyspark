"""
Docstring for bronze
"""
import os
import json
import glob
import logging
from typing import Optional
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
import pyspark.sql.functions as sf
import pyspark.errors as pe

# Set environment variables and other constants
load_dotenv()
SCHEMA_JSON_PATH = 'raw/schemas.json'
BRONZE_PATH = 'data/bronze'
RAW_PATH = 'raw'
LOG_PATH = 'log'

# Set Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
os.makedirs(LOG_PATH,exist_ok=True)
log_file_path = os.path.join(LOG_PATH,'bronze.log')
fh = logging.FileHandler(log_file_path)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(fh)

# Initialize SparkSession
spark = SparkSession.builder.appName("Retail_ETL_Pipeline")\
    .config("spark.sql.shuffle.partitions", "4").getOrCreate()
logger.info("Initialized SparkSession Retail_ETL_Pipeline")

# Load schema.json to dict
def get_schema_json(path:str='**/schemas.json') -> dict:
    # find schema path
    schema_path = glob.glob(pathname=path, recursive=True)
    try:
        # if no schema_path is found
        if len(schema_path) == 0:
            raise FileNotFoundError(f"Schema not found in {path}")
        # if multiple schema paths are found
        if len(schema_path) > 1:
            raise FileNotFoundError(f"Multiple schema files found in {path}")
        schema_path = schema_path[0]
        # read schema from path and parse as dict
        with open(schema_path,'r') as file:
            schema_dict = json.load(file)
            logger.info(f"Successfully parsed schema '{schema_path}' as dict")
        return schema_dict
    except FileNotFoundError as e:
        logger.exception(f"Unable to load schema: {e}", exc_info=True)
        raise e
    except json.JSONDecodeError as e:
        logger.exception(f"Unable to parse json: {e}", exc_info=True)
        raise e

# extract dataframe schema
def get_dataframe_schema(schema_dict:dict[str,list],table:str,sorting_key:str='column_position') -> StructType:
    table_schema:Optional[list[dict]] = schema_dict.get(table,None)
    if not table_schema:
        logger.error("Table not found in schema")
        return StructType([])
    logger.info(f"========== Parsing {table} ========== ")
    columns = []
    try:
        for i, col in enumerate(sorted(table_schema, key=lambda x: x[sorting_key])):
            col_name = col.get("column_name",f"unknown_col_{i}")
            data_type_name = f"{col.get('data_type','string').title()}Type"
            data_type_class = globals().get(data_type_name,'StringType')
            columns.append(StructField(col_name, data_type_class(),nullable=True))
            logger.info(f"Successfully parsed column {i + 1}: {col_name} of {len(table_schema)}")
        return StructType(columns)
    except Exception as e:
        logger.exception(f"Error parsing col: {col_name},{data_type_name}", exc_info=True)
        raise e

# load data into dataframe from files
def load_data(src_path:str, table:str,schema:StructType,pattern:str="part-*"):
    """
    """
    table_dir_path = os.path.join(src_path,table)
    search_path = os.path.join(table_dir_path,pattern)
    path_list = glob.glob(pathname=search_path,recursive=True)
    try:
        # raise FileNotFoundError no file exists
        if len(path_list) == 0:
            raise FileNotFoundError(f"File not found in {search_path}")

        # read files using Spark
        df = spark.read.csv(table_dir_path,schema=schema,sep=',')
        logger.info(f"Successfuly loaded {table} from directory: {table_dir_path}")
        return df
    except FileNotFoundError as e:
        logger.exception(f"Error loading data, {e}", exc_info=True)
        raise e
    except pe.ParseException as e:
        logger.exception(f"Error parsing {table_dir_path}: {e}", exc_info=True)
        raise e
    except Exception as e:
        logger.exception(f"An unhandled exception occurred: {e}", exc_info=True)
        raise e

# Data cleaning functions
def clean_customers(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning customers data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_customers')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_customers """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of customers before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # Drop nulls and duplicate IDs
    df = df.dropna(subset=['customer_id']).dropDuplicates(subset=['customer_id'])
    logger.debug("Dropped nulls and duplicate customer ids")
    # Dropping email and password PII
    columns = [col for col in df.columns if col not in ['customer_email','customer_password']]
    df_bronze = df.select(columns)
    logger.debug("Dropped customer email and password columns as PII")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of customers after bronze cleaning: {df_bronze.count()}")
    return df_bronze

def clean_departments(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning departments data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_departments')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN department_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_departments """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of departments before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # Drop nulls and duplicate IDs
    df = df.dropna(subset=['department_id']).dropDuplicates(subset=['department_id'])
    logger.debug("Dropped nulls and duplicate department ids")
    # Standardizing text in column
    df_bronze = df.withColumn("department_name",sf.trim(sf.initcap(df.department_name)))
    logger.debug("Trimmed and capitalized the department name for uniformity")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of departments after bronze cleaning: {df_bronze.count()}")
    return df_bronze

def clean_categories(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning categories data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_categories')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_categories """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of categories before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # Drop nulls and duplicate IDs
    df = df.dropna(subset=['category_id']).dropDuplicates(subset=['category_id'])
    logger.debug("Dropped nulls and duplicate category_ids")
    # Standardizing text in column
    df_bronze = df.withColumn("category_name",sf.trim(sf.initcap(df.category_name)))
    logger.debug("Trimmed and capitalized the category name for uniformity")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of categories after bronze cleaning: {df_bronze.count()}")
    return df_bronze

def clean_order_items(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning order_items data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_order_items')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_order_items """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of order_items before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # Drop nulls and duplicate IDs
    df = df.dropna(subset=['order_item_id']).dropDuplicates(subset=['order_item_id'])
    logger.debug("Dropped nulls and duplicate order_items ids")
    # converting data types from float to double for precision
    df_bronze = df.withColumns({'order_item_subtotal':sf.round(sf.col('order_item_subtotal').cast(DoubleType()),2),\
                                'order_item_product_price':sf.round(sf.col('order_item_product_price').cast(DoubleType()),2)})
    logger.debug("Converted float data types to double for greater precision")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of order_items table after bronze cleaning: {df_bronze.count()}")
    return df_bronze

def clean_orders(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning orders data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_orders')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_orders """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of orders before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # Drop nulls and duplicate IDs
    dfa = df.dropna(subset=['order_id']).dropDuplicates(subset=['order_id'])
    logger.debug("Dropped nulls and duplicate order ids")

    # Cleaned text and removed underscore in order_status
    df_bronze = dfa.withColumn("order_status", sf.initcap(sf.regexp_replace("order_status","_"," ")))
    logger.debug("Removed underscore in order_status and updated case to title case")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of orders table after bronze cleaning: {df_bronze.count()}")
    return df_bronze

def clean_products(df:DataFrame) -> DataFrame:
    logger.info("========== Cleaning products data ==========")

    # Calculate null counts and metrics via sql query
    df.createOrReplaceTempView('temp_products')
    counts_df = spark.sql("""
    SELECT COUNT(1) AS id_count,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM temp_products """)
    counts = counts_df.first()

    # Log counts of unique ids
    logger.info(f"Record count of products before cleaning: {counts.id_count}")
    logger.warning(f"Record count of null values in primary key before cleaning: {counts.null_count}")

    # removing unnecessary columns
    columns = [col for col in df.columns if col not in ['product_description','product_image']]
    logger.debug("Dropped unnecessary columns - product_description and product_image")
    # Drop nulls and duplicate IDs
    df = df.select(columns).dropna(subset=['product_id']).dropDuplicates(subset=['product_id'])
    logger.debug("Dropped nulls and duplicate product ids")
    # converting data types from float to double for precision
    df_bronze = df.withColumn("product_price",sf.round(df.product_price.cast(DoubleType()),2))
    logger.debug("Converted float data types to double for greater precision")

    # Log total number of records after bronze cleaning
    logger.info(f"Record count of products table after bronze cleaning: {df_bronze.count()}")
    return df_bronze


# Save dataframe as parquet
def save_as_bronze(df:DataFrame, name:str):
    logger.info(f"============ Saving {name} as Parquet ==========")
    try:
        # create save path
        save_path = os.path.join(BRONZE_PATH,f"{name}_clean.parquet")
        logger.debug(f"Created save path for {name}: '{save_path}'")
        # write to parquet
        df.write.parquet(path=save_path,mode='overwrite')
        logger.info(f"Successfully saved {name} to '{save_path}'")
    except (pe.PySparkException, OSError) as e:
        logger.error(f"Error saving {name}: {e}")

# Full bronze transformation pipeline
def _process_bronze_table(name:str):
    logger.info(f"========== Started processing {name} ==========")
    try:
        df = load_data(RAW_PATH, name, get_dataframe_schema(schema_dict,name))
        table_fn_map = {
            'customers': clean_customers,
            'departments': clean_departments,
            'categories': clean_categories,
            'order_items': clean_order_items,
            'orders': clean_orders,
            'products': clean_products,
        }
        cleaned = table_fn_map[name](df)
        save_as_bronze(cleaned,name)
        logger.info(f"========== Finished processing {name} ==========")
        return name
    except Exception as e:
        logger.exception(f"Error processing {name}: {e}")
        raise


# Use a thread pool to parallelize Spark jobs from the same SparkSession
schema_dict = get_schema_json(path=SCHEMA_JSON_PATH)

def main():
    tables = schema_dict.keys()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_process_bronze_table, t):t for t in tables}
        for fut in as_completed(futures):
            tbl = futures[fut]
            try:
                fut.result()
            except Exception as e:
                logger.critical(f"Table {tbl} failed: {e}")
                raise

if __name__ == "__main__":
    main()