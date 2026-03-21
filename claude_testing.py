from sqlalchemy import text
from teelo.db.session import get_session

with get_session() as s:
    rows = s.execute(text("""
        SELECT t.level, t.id, t.name, t.country, t.surface
        FROM tournaments t
        WHERE t.country_ioc IS NULL
        ORDER BY t.level, t.name
    """)).fetchall()
    
    current_level = None
    for r in rows:
        if r.level != current_level:
            current_level = r.level
            count = sum(1 for x in rows if x.level == current_level)
            print(f"\n=== {current_level} ({count} tournaments) ===")
        print(f"  {r.id}: {r.name} | country={r.country} | surface={r.surface}")
