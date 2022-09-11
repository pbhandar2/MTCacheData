import pathlib 
import copy


class UpdateOutput:
    def __init__(self, output_file_path, tag):
        self.output_file_path = pathlib.Path(output_file_path)
        self.data = []
        with self.output_file_path.open("r") as f:
            line = f.readline()
            while line:
                self.data.append(line.rstrip())
                if "generator" in line:
                    copy_line = copy.deepcopy(line)
                    copy_line = copy_line.replace("generator", "tag")
                    copy_line = copy_line.replace("multi-replay", tag)
                    self.data.append(copy_line.rstrip())
                line = f.readline()


    def write_to_file(self, file_path):
        with pathlib.Path(file_path).open("w+") as f:
            f.write("\n".join(self.data))
        

if __name__ == "__main__":
    output_update = UpdateOutput("../data/cloudlab_c/w82/256_16_1_593_934_0", "cloudlab_a")