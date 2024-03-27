import subprocess
import time
import argparse
import os
from datetime import datetime

def increase_memory_for_loadgenerator():
    try:
        cmd = "export HBASE_OPTS='-Xmx10g'"
        print(cmd)
        op = subprocess.call(cmd, shell=True)
        print("Output:", op)
    except subprocess.CalledProcessError as e:
        print("Error in setting HBASE_HEAPSIZE:", e)
        exit(1)
def login():
    try:
        cmd = "kinit -kt <systest.keytab> systest"
        print(cmd)
        login_op = subprocess.call(cmd, shell=True)
        print("Login output:", login_op)
    except subprocess.CalledProcessError as e:
        print("Error in login:", e)
        exit(1)

def create_ltt_command_multiput(num_cols_per_cf=1000, num_threads=10, num_keys=100, table_name = "multitest",avg_data_size=2, num_col_families=3, col_family_pattern="cf%d", num_regions_per_server=1):
    def get_column_families():
        col_families = []
        for i in range(num_col_families):
            col_families.append(col_family_pattern % i)
        return ','.join(col_families)
    #Sample: hbase ltt -tn multitest -families f1,f2,f3 -write 20000:2:20 -multiput -num_keys 1000 -num_regions_per_server 1
    cmd = f"hbase ltt -tn {table_name} -families {get_column_families()} -write {num_cols_per_cf}:{avg_data_size}:{num_threads}" \
          f" -multiput -num_keys {num_keys} -num_regions_per_server {num_regions_per_server}"
    return cmd


def create_pe_command_multiget(multiget_batchsize=500, num_threads=10, num_keys=100, table_name="multitest", num_col_families=3):
    #Sample: hbase pe --table=multitest --families=3 --columns=10000 --multiGet=10 --rows=1000 --nomapred randomRead 5

    cmd = f"hbase pe --table={table_name} --families={num_col_families} --columns={num_cols_per_cf} " \
          f"--multiGet={multiget_batchsize} --rows={num_keys} --nomapred randomRead {num_threads}"
    return cmd



def generate_hbase_load(op_type, multiget_batchsize, num_cf, num_rows_list, num_cols_per_cf, num_threads_list, metadata, csv_outfile="/root/ltt_output.csv", ):
    #if  output file does not exist only then write the header
    if(not os.path.exists(csv_outfile)):
        with open(csv_outfile, "w") as f:
            f.write("op,num_cf,num_keys,num_cols_per_cf,num_threads,time_taken,command,metadata,date_start,time_start,date_end,time_end\n")
    assert type(num_threads_list) == list
    assert type(num_rows_list) == list
    for num_keys in num_rows_list:
        for num_threads in num_threads_list:
            if op_type == "multiput":
                cmd = create_ltt_command_multiput(num_cols_per_cf=num_cols_per_cf,
                                                  num_threads=num_threads,
                                                  num_keys=num_keys,
                                                  num_col_families=num_cf)
            elif op_type == "multiget":
                cmd = create_pe_command_multiget(multiget_batchsize=multiget_batchsize,
                                                 num_threads=num_threads,
                                                 num_keys=num_keys,
                                                 num_col_families=num_cf)
            else:
                print("Invalid op_type")
                exit(1)

            datetime_start = datetime.now()
            date_start_str = datetime_start.date()
            time_start_str = str(datetime_start.time()).split(".")[0]
            time_start = time.time()
            ltt_out = subprocess.call(cmd, shell=True)
            time_end = time.time()
            datetime_end = datetime.now()
            date_end_str = datetime_end.date()
            time_end_str = str(datetime_end.time()).split(".")[0]
            time_taken = time_end - time_start

            print("cmd:", cmd)
            print("LTT output:", ltt_out)
            print("Time taken:", time_taken)
            with open(csv_outfile, "a") as f:
                if ltt_out != 0:
                    time_taken = "non_zero_exit_code"
                f.write(f'{op_type},{num_cf},{num_keys},{num_cols_per_cf},{num_threads},{time_taken},"{cmd}",{metadata},{date_start_str},{time_start_str},{date_end_str},{time_end_str}\n')
                print(f"Written to file: {csv_outfile}")
            # Sleep added so that the next command does not start immediately and any metric measurement such as heap useage can be captured more accurately
            time.sleep(90)

if __name__ == '__main__':
    argparser = argparse.ArgumentParser("Generate LTT load and create report")
    argparser.add_argument('-csv_output', '--csv_output', help='Full path to the csv output file', default="/root/ltt_output.csv", required=False)
    argparser.add_argument('-metadata', '--metadata', help='Metadata to be added to the output file', default="no_cmd_line_metadata", required=False)
    argparser.add_argument('-op_type', '--op_type', help='Type of operation to perform (multiget/multiput)', default="multiput", required=False)
    args = argparser.parse_args()
    increase_memory_for_loadgenerator()
    login()
    num_cf = 3
    num_rows_list = [10,100,1000]
    multiget_batchsize = 10
    num_cols_per_cf = 10000
    num_threads_list = [5]
    generate_hbase_load(args.op_type, multiget_batchsize, num_cf, num_rows_list, num_cols_per_cf, num_threads_list, args.metadata, args.csv_output)
    print("Done")
