from .utils.utils import save_db_as_json
def export_json(out_path='./output/output.json') -> None:
    """
    Export data stored in internal dict into a JSON file
    :param output_path: where the file should be stored
    :type output_path: str, optional
    :return: 
    """
    from .utils.utils import global_db

    global_db_hold = global_db.copy()

    fileout = out_path
    save_db_as_json(filename=fileout, dict_db=global_db_hold)

