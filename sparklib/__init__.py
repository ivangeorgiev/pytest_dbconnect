#%%
from pyspark import sql as pss 
import pyspark.sql.functions as pssf
import pyspark.sql.types as psst

def sum(df:pss.DataFrame, field_name):
    return df.groupBy().sum(field_name).collect()[0][0]

def to_dict(df:pss.DataFrame, sort_by_list=None):
    pd_df = df.toPandas()
    pd_df = pd_df.sort_values(sort_by_list).reset_index(drop=True)
    return pd_df.to_dict('records')

def to_list(input_df, deep=True, **kwargs):
    """Convert Spark DataFrame to list"""
    def row_as_dict(r):
      d = r.asDict()
      if deep:
        for k in d:
          if isinstance(d[k], psst.Row):
            d[k] = row_as_dict(d[k])
      return d
    
    if isinstance(input_df, pss.DataFrame):
        result = [row_as_dict(r) for r in input_df.collect()]
    else:
        result = input_df
    return result



def tr_find_duplicates(input_df, key, order=None, error_column="error", message="Duplicate key."):
    """Find duplicates in Spark DataFrame."""
    win = pss.Window.partitionBy(key)
    if not order:
        order = key
    win = win.orderBy(order)
    dup_num_column = "artemis_dup_no___"
    result_df = (input_df
        .withColumn(dup_num_column, pssf.row_number().over(win))
        .withColumn(error_column, pssf.when(pssf.col(dup_num_column) > 1, message))
        .drop(dup_num_column)
    )
    return result_df


# %%
