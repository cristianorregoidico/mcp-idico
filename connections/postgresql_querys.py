def pending_guides_query() -> str:
    """
    Devuelve una consulta SQL para obtener guías pendientes de PostgreSQL.
    """
    return """
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
WHERE gh.status_envio <> 'GUIA ENTREGADA';
    """
    
def get_ob_time_delivery(initial_date: str, final_date: str, so_number: str = None) -> str:
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