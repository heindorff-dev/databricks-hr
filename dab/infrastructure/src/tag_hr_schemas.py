# Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schemas", "")

catalog = dbutils.widgets.get("catalog")
schemas = [s.strip() for s in dbutils.widgets.get("schemas").split(",") if s.strip()]

for schema in schemas:
    spark.sql(f"ALTER SCHEMA `{catalog}`.`{schema}` SET TAGS ('People and Culture')")
