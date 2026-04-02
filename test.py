from databricks.connect import DatabricksSession
import os

# Forzamos el Cluster ID si no lo pusiste en la terminal
# cluster_id = "0123-456789-abcde123" # <--- Descomenta y pon el tuyo si falla

try:
    spark = DatabricksSession.builder.getOrCreate()
    print("------------------------------------------")
    print("¡CONEXIÓN EXITOSA CON AZURE DATABRICKS!")
    print(f"Sesión activa en el clúster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterId')}")
    print("------------------------------------------")
    spark.range(3).show()
except Exception as e:
    print(f"Error en la conexión: {e}")