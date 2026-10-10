# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("success_factor_bronze_schema", "")
dbutils.widgets.text("success_factor_silver_schema", "")
dbutils.widgets.text("success_factor_gold_schema", "")

catalog = dbutils.widgets.get("catalog")
success_factor_bronze_schema = dbutils.widgets.get("success_factor_bronze_schema")
success_factor_silver_schema = dbutils.widgets.get("success_factor_silver_schema")
success_factor_gold_schema = dbutils.widgets.get("success_factor_gold_schema")

spark.sql(f"ALTER SCHEMA {catalog}.{success_factor_bronze_schema} SET TAGS ('People and Culture')")
spark.sql(f"ALTER SCHEMA {catalog}.{success_factor_silver_schema} SET TAGS ('People and Culture')")
spark.sql(f"ALTER SCHEMA {catalog}.{success_factor_gold_schema} SET TAGS ('People and Culture')")
