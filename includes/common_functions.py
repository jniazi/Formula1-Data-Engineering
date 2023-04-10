# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrance_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output = input_df.select(column_list)
    return output

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output = re_arrance_partition_column(input_df, partition_column)

    # by default the overwrite method is static. we need to make it dynamic and tell the spark to replace only those records that already exist
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # first we check if the table exists, use insertInto method otherwise the saveAsTable method
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                            .distinct() \
                            .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------


def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    """ 
    parameters:
    merge_condition: since we need to add the partition column in the merge condition in order that the spark find the records faster, we use the parameter
    """
    # since the merge operation is expensive, we need to add the partition column in the merge condition and 
    # the following config is to tell the spark to use the dynamic partition
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------


