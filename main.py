import tnl_engagement_metrics_times_cs as tnl
import time
import yaml
import os
import sys


def engagement_metrics(env):
    DAG_CONFIG = yaml.load(open(os.path.join(os.path.dirname(__file__), "config/config_%s.yaml" % env.lower())))
    project = DAG_CONFIG['environment']['project']
    #dataset_input = DAG_CONFIG['environment']['dataset_input']
    dataset_output = DAG_CONFIG['environment']['dataset_output']
    output_table1 = DAG_CONFIG['environment']['output_table1']
    output_table2 = DAG_CONFIG['environment']['output_table2']
    output_table_gcs1 = DAG_CONFIG['environment']['output_table_gcs1']
    output_table_gcs2 = DAG_CONFIG['environment']['output_table_gcs2']
    output_gcs_storage = DAG_CONFIG['environment']['output_gcs_storage']
    tnl.run_all(project, dataset_output, output_table1, output_table2, output_table_gcs1,output_table_gcs2)
    time.sleep(60)


if __name__ == "__main__":
    env = sys.argv[1]
    engagement_metrics(env)
