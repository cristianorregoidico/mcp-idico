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

def get_vendors_customer_brand(customer_name: str, brand: str) -> str:
    """
    Devuelve una consulta SQL para obtener la tasa de acierto de desvío para un cliente y marca específicos en PostgreSQL.
    """
    return f"""
    SELECT * FROM ods.analytics.hr_cus_brand_consolidado 
    WHERE customer_name LIKE '%' || UPPER('{customer_name}') || '%'
    AND brand LIKE '%' || UPPER('{brand}') || '%'
    AND probabilidad > 0
    ORDER BY 
    customer_name ASC,
    brand ASC,
    probabilidad DESC,
    count_so DESC;
    """
    
def get_vendors_country_brand(country: str, brand: str) -> str:
    """
    Devuelve una consulta SQL para obtener la tasa de acierto de desvío para un país y marca específicos en PostgreSQL.
    """
    return f"""
    SELECT * FROM ods.analytics.hr_country_brand_consolidado
    WHERE country LIKE '%' || UPPER('{country}') || '%'
    AND brand LIKE '%' || UPPER('{brand}') || '%'
    ORDER BY 
        country ASC,
        brand ASC,
        probabilidad DESC,
        count_so DESC;
    """

def get_customer_country(customer_name: str) -> str:
    """
    Devuelve una consulta SQL para obtener el país de un cliente específico en PostgreSQL.
    """
    return f"""
    SELECT country FROM ods.analytics.hr_cus_brand_consolidado
    WHERE customer_name LIKE '%' || UPPER('{customer_name}') || '%'
    LIMIT 1;
    """
    
def get_calls_summary(start_date: str, final_date: str, customer_name: str = '', organizer: str = '', subject: str = '') -> str:
    """
    Devuelve una consulta SQL para obtener el resumen de llamadas entre dos fechas específicas en PostgreSQL.
    """
    return f"""
    SELECT
    activity_date,
    subject,
    account,
    organizer,
    attendees,
    contact,
    description
FROM ods.analytics.dataset_modjo_idra
WHERE activity_date >= '{start_date}'
  AND activity_date < '{final_date}'
  AND description IS NOT NULL
  AND LENGTH(TRIM(description)) >= 500
  AND ('{customer_name}' IS NULL OR UPPER(account) ILIKE '%' || UPPER('{customer_name}') || '%')
  AND ('{organizer}' IS NULL OR UPPER(organizer) ILIKE '%' || UPPER('{organizer}') || '%')
  AND (
        '{subject}' IS NULL
        OR UPPER(subject) ILIKE '%' || UPPER('{subject}') || '%'
      )
ORDER BY activity_date DESC;
    """