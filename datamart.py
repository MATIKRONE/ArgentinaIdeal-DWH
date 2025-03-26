from google.cloud import bigquery

PROJECT_ID = "usm-infra-grupo8-401213"

if __name__ == '__main__':
    view_names =  ['view_total_ventas_negocio_provincia', 'view_total_ventas_productos']
    sql_view_total_ventas_negocio_provincia = """CREATE VIEW `{PROJECT_ID}.dm_ventas.view_total_ventas_negocio_provincia` AS
            SELECT clientes.tipo_negocio AS negocio, clientes.provincia AS provincia, ROUND(SUM(ventas.venta_importe),2) AS importe_total
            FROM {PROJECT_ID}.dwh_ventas.fact_table_venta AS ventas
            LEFT JOIN {PROJECT_ID}.dwh_ventas.dim_table_cliente AS clientes
            ON ventas.codigo_cliente = clientes.codigo_cliente
            GROUP BY clientes.provincia, clientes.tipo_negocio;""".format(PROJECT_ID=PROJECT_ID)

    sql_view_total_ventas_productos = """CREATE VIEW `{PROJECT_ID}.dm_ventas.view_total_ventas_productos` AS
            SELECT productos.SKU_descripcion, SUM(ventas.venta_unidades) AS ventas_unidades
            FROM {PROJECT_ID}.dwh_ventas.fact_table_venta AS ventas
            LEFT JOIN {PROJECT_ID}.dwh_ventas.dim_table_producto AS productos
            ON ventas.SKU_codigo = productos.SKU_codigo
            GROUP BY productos.SKU_descripcion;""".format(PROJECT_ID=PROJECT_ID)

    for v in view_names:
        client = bigquery.Client()
        client.query(locals()[f"sql_{v}"])