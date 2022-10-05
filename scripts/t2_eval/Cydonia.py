import pathlib 
from mtDB.cydonia.Cydonia import Cydonia


TEST_ST_1 = pathlib.Path.home().joinpath("mtdata/c220g1/w82/128_16_100_800_0_0")
TEST_MT_1 = pathlib.Path.home().joinpath("mtdata/c220g1/w82/128_16_100_800_1600_0")
WORKLOAD_1 = "w82"


# TEST_ST_2 = pathlib.Path.home().joinpath("mtdata/r6525/w82/128_16_100_800_0_0")
# TEST_MT_2 = pathlib.Path.home().joinpath("mtdata/r6525/w82/128_16_100_800_1600_0")
# WORKLOAD_2 = "w82"

# TEST_ST_2 = pathlib.Path.home().joinpath("mtdata/sgdp7/w82/128_16_100_800_0_0")
# TEST_MT_2 = pathlib.Path.home().joinpath("mtdata/sgdp7/w82/128_16_100_800_1600_0")
# WORKLOAD_2 = "w82"


if __name__ == "__main__":
    cydonia = Cydonia(TEST_ST_1, TEST_MT_1, WORKLOAD_1)
    cydonia.run()