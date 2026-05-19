import mysql.connector, base64, json, os

with open(os.path.join(os.path.dirname(__file__), '..', 'config.json')) as f:
    cfg = list(json.load(f).values())[0]
pw = base64.b64decode(cfg['mysql_pass_b64']).decode()

conn = mysql.connector.connect(
    host=cfg['mysql_host'], port=int(cfg['mysql_port']),
    user=cfg['mysql_user'], password=pw,
    database=cfg['mysql_db'], connection_timeout=5,
)
cur = conn.cursor()

# Alle Einträge für "Prod Test"
cur.execute("""
    SELECT tp.Id, tp.Layer, tp.PlatingId, tp.ProductId, pl.ProductName
    FROM TableProduct tp
    LEFT JOIN TableProductList pl ON tp.ProductId = pl.Id
    WHERE pl.ProductName = 'Prod Test'
    ORDER BY tp.Layer, tp.PlatingId
""")
rows = cur.fetchall()
print(f"{'Id':4} | {'Layer':5} | {'PlatingId':9} | {'ProductId':9} | Product")
print("-" * 55)
for r in rows:
    print(f"{str(r[0]):4} | {str(r[1]):5} | {str(r[2]):9} | {str(r[3]):9} | {r[4]}")
print(f"\n{len(rows)} Zeilen gefunden.")

# Duplikate: gleicher Layer + gleiche ProductId (verschiedene PlatingId)
cur.execute("""
    SELECT tp.Layer, tp.ProductId, COUNT(*) AS cnt,
           GROUP_CONCAT(tp.PlatingId ORDER BY tp.PlatingId) AS platings
    FROM TableProduct tp
    LEFT JOIN TableProductList pl ON tp.ProductId = pl.Id
    WHERE pl.ProductName = 'Prod Test'
    GROUP BY tp.Layer, tp.ProductId
    HAVING COUNT(*) > 1
""")
dupes = cur.fetchall()
if dupes:
    print(f"\n>>> {len(dupes)} Duplikat(e): gleicher Layer + ProductId, verschiedene PlatingIds:")
    for d in dupes:
        print(f"  Layer={d[0]}, ProductId={d[1]}, Anzahl={d[2]}, PlatingIds=[{d[3]}]")
else:
    print("\n>>> Keine Duplikate (gleicher Layer + ProductId) gefunden.")

conn.close()
