from pathlib import Path


def recreate_all_views(conn, views_dir=None):
    """
    Recreate all views found in a directory of .sql files.
    Executes each file in alphabetical order.
    """
    if views_dir is None:
        views_dir = Path(__file__).parent / "views"

    with conn.cursor() as cursor:
        for sql_file in sorted(views_dir.glob("*.sql")):
            with open(sql_file, "r") as f:
                sql = f.read().strip()
                if not sql:
                    continue
                print(f"Recreating view: {sql_file.stem}")
                cursor.execute(sql)
    conn.commit()
    print("✅ All views successfully recreated.")
