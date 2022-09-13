import pathlib 
from mtDB.db.DBUnit import DBUnit


class MTDB:
    def __init__(self, data_dir):
        self.data_dir = pathlib.Path(data_dir)
        self.unit_list = []
        self._load_data()


    def _get_id(self, db_unit_path):        
        return db_unit_path.name , db_unit_path.parent.name


    def _load_data(self):
        db_unit_path_list = []
        for data_file_path in self.data_dir.rglob("*"):
            if data_file_path.is_file():
                db_unit_path = data_file_path.parent
                workload_id, machine_id = self._get_id(db_unit_path)
                if str(db_unit_path) not in db_unit_path_list:
                    db_unit_path_list.append(str(db_unit_path))
                    self.unit_list.append(DBUnit(db_unit_path, machine_id, workload_id))

    
    def get_opt_count(self):
        st_opt_count, mt_opt_count, np_mt_opt_count = 0, 0, 0 
        for db_unit in self.unit_list:
            st_opt_count += db_unit.st_opt_count 
            mt_opt_count += db_unit.mt_opt_count 
            np_mt_opt_count += db_unit.np_mt_opt_count 
        
        return st_opt_count, mt_opt_count, np_mt_opt_count
