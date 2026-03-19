def mkdirs(path: str, dbutils) -> None:
    """
    Create directory in DBFS / Unity Catalog volume.
    """
    dbutils.fs.mkdirs(path)


def write_json(path: str, data: str, dbutils) -> None:
    """
    Write JSON string to DBFS / volume.
    """
    dbutils.fs.put(path, data, overwrite=True)