import pandas as pd
from typing import *

class Read_csv(object):
    '''
    Class to perform various operations on the given .csv file
    '''
    def __init__(self, filename):
        self.filename = filename
        
    def column_list(self):
        '''
        Class to parse all the columns in the dataframe and converting it into appropriate lists
        '''
        
        
        self.df = pd.read_csv(self.filename)
        self.df['parents'].fillna(0, inplace = True)
        self.txid = list()
        self.fee = list() #maximize
        self.wt = list()  #minimize within max weight
        self.parents = list()

        for i in self.df.index:
            self.txid.append(self.df['tx_id'][i])
            self.fee.append(self.df['fee'][i])
            self.wt.append(self.df['weight'][i])
            if self.df['parents'][i] != 0:
                p = self.df['parents'][i]
                pl = p.split(";")
                self.parents.append(pl)
            else:
                self.parents.append(0)
                
        return self.txid, self.fee, self.wt, self.parents
    
    def fee_per_weight(self):
        '''
        Getting the fee / weight ratio for each transaction
        '''
        
        self.res = [i / j for i, j in zip(self.fee, self.wt)]
        return self.res
    
    def sorted(self):
        '''
        Sorting the <res> list in descending order along with the other lists
        '''
        for i in range(len(self.res)):	      
            # Find the minimum element in remaining 
            # unsorted array
            min_idx = i
            for j in range(i+1, len(self.res)):
                if self.res[min_idx] < self.res[j]:
                    min_idx = j
                    
            # Swap the found minimum element with 
            # the first element        
            self.res[i], self.res[min_idx] = self.res[min_idx], self.res[i]
            self.txid[i], self.txid[min_idx] = self.txid[min_idx], self.txid[i]
            self.fee[i], self.fee[min_idx] = self.fee[min_idx], self.fee[i]
            self.wt[i], self.wt[min_idx] = self.wt[min_idx], self.wt[i]
            self.parents[i], self.parents[min_idx] = self.parents[min_idx], self.parents[i]
            
        return self.txid, self.fee, self.wt, self.parents, self.res
    
class Get_Blocks(Read_csv):
    def get_init(self):
        self.net_wt = 0
        self.ids = list()
        self.net_fee = 0
        
    @staticmethod
    def check_parents(parent_ids, ids):
        '''
        Checks if the parents of the current tranasction exists in the block(ids)
        '''
        parent_bool = True
        for parent_id in parent_ids: 
            if parent_id not in ids:
                parent_bool = False
        
        return parent_bool
    
    def get_ids(self):
        '''
        Obtaining the maximum transaction:
        -> Checking if the net weight after adding next transaction exceeds the LIMIT
        -> Checking if the parents of the trnasaction are there in the block.
        -> Adding the weight and fee, if all conditions satisfy
        '''
        for i in range(len(self.res)):
            if self.net_wt + self.wt[i] >= 4000000:
                continue # continue added to ensure we reach the nearest value to the WEIGHT LIMIT
            
            if self.parents[i] != 0:
                if not self.check_parents(self.parents[i], self.ids):                    
                    continue                        
                    
            self.net_wt += self.wt[i]
            self.net_fee += self.fee[i]
            self.ids.append(self.txid[i])               
                    
        
        return self.net_wt,self.net_fee, self.ids            
        
    def write_to_csv(self):
        '''
        Exporting the Transaction ID list in form of a .txt file
        '''
        textfile = open("block.txt", "w")  # Opening .txt file with write permission            
        for t_id in self.ids:
	        textfile.write(t_id + "\n")
        textfile.close()   
        


        
        

