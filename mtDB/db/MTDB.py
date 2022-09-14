import pathlib 
from mtDB.db.DBUnit import DBUnit


class MTDB:
    def __init__(self, data_dir):
        self.data_dir = pathlib.Path(data_dir)
        self.unit_list = []
        self._load_data()


    def _get_id(self, db_unit_path):    
        # the subdir containing data files has path of format-> MACHINE/WORKLOAD/DATAFILE    
        return db_unit_path.name , db_unit_path.parent.name


    def _load_data(self):
        # load all files inside the data directory 
        # data from files in the same subdir is stored as a single unit DBUnit
        # create a single DBUnit per subdir that contain data files 
        # so we track in a list whether a given subdir has already been loaded as a DBUnit
        db_unit_path_list = []
        for data_file_path in self.data_dir.rglob("*"):
            if data_file_path.is_file():
                db_unit_path = data_file_path.parent
                workload_id, machine_id = self._get_id(db_unit_path)
                if str(db_unit_path) not in db_unit_path_list:
                    db_unit_path_list.append(str(db_unit_path))
                    db_unit = DBUnit(db_unit_path, machine_id, workload_id)
                    if db_unit.get_size() > 0:
                        self.unit_list.append(db_unit)


    def _get_combined_df(self):
        # the percentage difference in stats between MT and its corresponding 
        # ST cache from all eligible points 
        df = None 
        for db_unit in self.unit_list:
            if df is None:
                df = db_unit.get_diff_df()
            else:
                df = df.append(db_unit.get_diff_df(), ignore_index=True) 
        return df 


    def get_opt_count(self):
        st_opt_count, mt_opt_count, np_mt_opt_count = 0, 0, 0 
        for db_unit in self.unit_list:
            st_opt_count += db_unit.st_opt_count 
            mt_opt_count += db_unit.mt_opt_count 
            np_mt_opt_count += db_unit.np_mt_opt_count 
        
        return st_opt_count, mt_opt_count, np_mt_opt_count
