def get_helga_guides_query(po: str | None, status: str | None, service: str | None) -> tuple[str, list[str]]:
    where_clauses = []
    params: list[str] = []

    if status:
        where_clauses.append("gh.status_envio ILIKE %s")
        params.append(f"%{status}%")
    else:
        where_clauses.append("gh.status_envio <> 'GUIA ENTREGADA'")

    if po:
        where_clauses.append("gh.po ILIKE %s")
        params.append(f"%{po}%")
    if service:
        where_clauses.append("gh.servicio ILIKE %s")
        params.append(f"%{service}%")

    where_sql = " AND ".join(where_clauses)
    if where_sql:
        where_sql = "WHERE " + where_sql

    sql = f"""
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
    return sql, params
