#!/usr/bin/env python3
import os 
import pickle
dummy_info_path = "~/students/2020210977huojiaqi/spark/dummies/dummy_info_airdelay.pkl"
dummy_info = pickle.load(open(os.path.expanduser(dummy_info_path), "rb"))
print(dummy_info['factor_selected'])
print(dummy_info['factor_dropped'])
