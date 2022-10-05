import argparse 
import pandas as pd 

if __name__ == "__main__":
    df = pd.read_csv("cp_block.csv")
    
    df = df[df["workload"].isin(["w77", "w82", "w97", "w98", "w85"])]

    print(df.T.to_string())