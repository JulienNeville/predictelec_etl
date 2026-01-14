#outils sql

"""python_to_sql(value): Convertit une valeur Python en représentation SQL sécurisée."""
def python_to_sql(value):
    """Convertit une valeur Python en représentation SQL sécurisée."""
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # Échappement des apostrophes pour éviter l'injection SQL
        return "'" + value.replace("'", "''") + "'"
    else:
        return str(value)
    
def log_import(type,region,jour,status,message,conn):
    if type == 'PROD':
        log_import_production(conn,region,jour,status,message)

def log_import_production(conn, region, day, status, message):
    sql = """
        INSERT INTO import_log_production (
            code_insee_region,
            date_jour,
            status,
            message
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (code_insee_region, date_jour)
        DO UPDATE SET
            status = EXCLUDED.status,
            message = EXCLUDED.message,
            created_at = now()
    """

    with conn.cursor() as cur:
        cur.execute(sql, (region, day, status, message))
        conn.commit()