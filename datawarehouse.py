from google.cloud import bigquery

PROJECT_ID = "usm-infra-grupo8-401213"
 
def create_table(PROJECT_ID, TARGET_TABLE_ID,SQL,WRT_DISPOSITION):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
    destination=TARGET_TABLE_ID,
    write_disposition= WRT_DISPOSITION)
    query_job = client.query(SQL, job_config=job_config)
    try:
        query_job.result()
        print("Query success")
    except Exception as exception:
            print(exception)

 
if __name__ == "__main__":
    table_fact_names = ['fact_table_venta']
    table_dim_names = ['dim_table_cliente', 'dim_table_producto'] 
    ds_dwh = "dwh_ventas"
    sql_fact_table_venta= """ 
    SELECT 
        ventas.codigo_cliente,
        stock.SKU_codigo,
        ventas.venta_unidades,
        ventas.venta_importe
    FROM usm-infra-grupo8-401213.raw_ventas.venta AS ventas LEFT JOIN usm-infra-grupo8-401213.raw_ventas.stock AS stock ON ventas.SKU_codigo = stock.SKU_codigo;
    """

    sql_dim_table_cliente = """
    SELECT 
        clientes.codigo_cliente,
        clientes.tipo_negocio,
        clientes.provincia
    FROM usm-infra-grupo8-401213.raw_ventas.cliente AS clientes;
    """

    sql_dim_table_producto = """
    SELECT 
        stock.SKU_codigo,
        stock.SKU_descripcion
    FROM usm-infra-grupo8-401213.raw_ventas.stock AS stock;
    """

    for t_fact in table_fact_names:
        TABLE_ID = f"usm-infra-grupo8-401213.{ds_dwh}.{t_fact}"
        print(f"FACT_TABLE======", TABLE_ID)
        create_table("usm-infra-grupo8-401213",TABLE_ID,locals()[f"sql_{t_fact}"],"WRITE_APPEND")

    for t_dim in table_dim_names:
        TABLE_ID = f"{PROJECT_ID}.{ds_dwh}.{t_dim}"
        print("DIM_TABLE=====",TABLE_ID)
        create_table("usm-infra-grupo8-401213",TABLE_ID, locals()[f"sql_{t_dim}"],"WRITE_TRUNCATE")