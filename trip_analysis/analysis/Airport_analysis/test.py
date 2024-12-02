import os

def check_access():
    path = '/home/walkingtree/odysse_v2/dags/utils/odysse_poc_v2/trip_analysis/analysis/Airport_analysis/transform_data'
    print("Testing access to path:", path)
    print("Path exists:", os.path.exists(path))
    print("Writable:", os.access(path, os.W_OK))
    print("Readable:", os.access(path, os.R_OK))

check_access()
