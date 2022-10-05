import pathlib 
from collections import defaultdict


class Status:
    def __init__(self, data_dir=pathlib.Path.home().joinpath("mtdata")):
        self.data_dir = pathlib.Path(data_dir)
        self.machine_count = defaultdict(int)
        self._load()
    

    def _load(self):
        for machine_dir in self.data_dir.iterdir():
            machine_id = machine_dir.name
            for workload_dir in machine_dir.iterdir():
                self.machine_count[machine_id] += len(list(workload_dir.iterdir()))
    

    def print_machine_count(self):
        machine_id_list = self.machine_count.keys()
        machine_id_list = sorted(machine_id_list)
        for machine_id in machine_id_list:
            print("{:15s} - {}".format(machine_id, self.machine_count[machine_id]))
                    
    
if __name__ == "__main__":
    status = Status()
    status.print_machine_count()
