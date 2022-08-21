import argparse 
import pathlib 

from ExperimentOutput import ExperimentOutput


def print_experiment_stats(experiment_output_file):
    experiment_output = ExperimentOutput(experiment_output_file)
    print(experiment_output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display metrics from experiment output")
    parser.add_argument("--o", 
                            default=pathlib.Path("data/cloudlab_a/w105/128_8_1_8538_12242_0.dump"),
                            type=pathlib.Path, 
                            help="Path to an experiment stat dump file")
    args = parser.parse_args()

    print_experiment_stats(args.o)