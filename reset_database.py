"""Reset the database to a fresh state.

WARNING: This will delete all your measurements!
A backup will be created before deletion. Close Sporely before running it.
"""
from database.migrate import snapshot_sqlite_database
from database.schema import get_database_path


def reset_database():
    """Delete the database and create a fresh one."""
    database_path = get_database_path()
    if database_path.exists():
        # Create backup (includes changes still in the WAL sidecar)
        backup_path = database_path.with_suffix('.db.old')
        snapshot_sqlite_database(database_path, backup_path)
        print(f"Backup created at: {backup_path}")

        # Delete database and its WAL sidecars, which belong to the old file
        for path in (
            database_path,
            database_path.with_name(database_path.name + "-wal"),
            database_path.with_name(database_path.name + "-shm"),
        ):
            path.unlink(missing_ok=True)
        print("Deleted old database")

    # Initialize fresh database
    from database.schema import init_database
    init_database()
    print("\nFresh database created successfully!")
    print("\nYou can now run the application: python main.py")


if __name__ == "__main__":
    response = input(
        "This will DELETE all measurements and reset the database. "
        "Close Sporely first. Continue? (yes/no): "
    )
    if response.lower() in ['yes', 'y']:
        reset_database()
    else:
        print("Cancelled.")
