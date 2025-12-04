# import os
# from pathlib import Path

# # Base paths
# PROJECT_ROOT = Path(__file__).parent
# JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home"
# SPARK_HOME = os.path.expanduser("~/Spark")

# # Environment setup
# def setup_environment():
#     """Initialize all environment variables for PySpark"""
#     os.environ['JAVA_HOME'] = JAVA_HOME
#     os.environ['SPARK_HOME'] = SPARK_HOME
#     os.environ['PYSPARK_PYTHON'] = 'python'
#     os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

# # PySpark config
# SPARK_CONFIG = {
#     "spark.app.name": "Retail_ETL_Pipeline",
#     "spark.driver.memory": "2g",
#     "spark.executor.memory": "2g",
#     "spark.sql.shuffle.partitions": "200",
# }

# from config import setup_environment, SPARK_CONFIG
# from pyspark.sql import SparkSession

# # Setup environment once
# setup_environment()

# # Create session with config
# spark = SparkSession.builder
# for key, value in SPARK_CONFIG.items():
#     spark = spark.config(key, value)

# spark = spark.getOrCreate()
# print("✓ Spark session ready")