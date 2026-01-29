def get_helga_guides_query(po: str | None, status: str | None, service: str | None) -> str:
    where_clauses = []

    if status:
        where_clauses.append("gh.status_envio ILIKE '%{}%'".format(status))
    else:
        where_clauses.append("gh.status_envio <> 'GUIA ENTREGADA'")   
    
    if po:
        where_clauses.append("gh.po ILIKE '%{}%'".format(po))
    if service:
        where_clauses.append("gh.servicio ILIKE '%{}%'".format(service))

    where_sql = " AND ".join(where_clauses)
    if where_sql:
        where_sql = "WHERE " + where_sql

    return f"""
    SELECT 
        gh.po AS po_number,
        gh.origen_pais AS country_origin,
        gh.origen_ciudad AS city_origin,
        gh.destino_pais AS destination_country,
        gh.destino_ciudad AS destination_city,
        gh.fecha AS order_date,
        gh.fecha_recoleccion AS recollection_date,
        gh.servicio AS service,
        gh.numero_guia AS track_number,
        gh.referencia_envio AS reference,
        gh.destino_name AS destination_name,
        gh.origen_name AS origin_name,
        gh.status_envio AS status
    FROM ods.applications.guia_helga gh
    {where_sql};
    """
    
def get_on_time_delivery(initial_date: str, final_date: str, so_number: str = None) -> str:
    """
    Devuelve una consulta SQL para obtener el tiempo de entrega de las órdenes de compra en PostgreSQL.
    """
    if so_number:
        return f"""
        SELECT *
        FROM ods.analytics.tableau_otd otd
        WHERE otd.so_doc_number = '{so_number}';
        """
    
      
    return f"""
    SELECT 
    * 
    FROM ods.analytics.tableau_otd otd
    WHERE to_date(otd.if_create_date, 'YYYY/MM/DD')
        BETWEEN DATE '{initial_date}' AND DATE '{final_date}';
    """
    
def get_scorecard_by_is_month(inside_sales: str = None) -> str:
    """
    Devuelve una consulta SQL para obtener el scorecard por IS en PostgreSQL.
    """
    if inside_sales:
        return f"""
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_mensual WHERE sales_rep = '{inside_sales}';
        """
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_mensual;
    """
    
def get_scorecard_by_is_daily(inside_sales: str = None) -> str:
    """
    Devuelve una consulta SQL para obtener el scorecard por IS en PostgreSQL.
    """
    if inside_sales:
        return f"""
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_diario WHERE sales_rep = '{inside_sales}';
        """
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_diario;
    """
    
def get_scorecard_by_is_year(inside_sales: str = None) -> str:
    """
    Devuelve una consulta SQL para obtener el scorecard por IS en PostgreSQL.
    """
    if inside_sales:
        return f"""
        SELECT * FROM ods.analytics.tableau_scorecard_by_inside_anual WHERE sales_rep = '{inside_sales}';
        """
    return """
    SELECT * FROM ods.analytics.tableau_scorecard_by_inside_anual;
    """

def get_customer_imports_data(customer_name: str) -> str:
    """
    Devuelve una consulta SQL para obtener las importaciones de un cliente específico en PostgreSQL.
    """
    return f"""
    SELECT * FROM ods.analytics.datasur WHERE importador LIKE '%{customer_name}%';
    """