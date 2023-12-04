from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# Spark UDF for converting monetary values
@udf(returnType=FloatType())
def convert_monetary_values(value):
    if value is None:
        return 20e6
    value = value.replace(' million', '').replace('$', '').replace(',', '')
    try:
        return float(value) * 1e6 if 'million' in value else float(value)
    except ValueError:
        return 0.0

