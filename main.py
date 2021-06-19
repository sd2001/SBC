from solution import *

# Calling and importing all the Class methods
    
obj = Get_Blocks('./mempool.csv')
txid, fee, wt, parents = obj.column_list()   
res = obj.fee_per_weight()

txid, fee, wt, parents, res = obj.sorted()
obj.get_init()

net_wt, net_fee, ids = obj.get_ids()
obj.write_to_csv()
print(net_wt, net_fee)