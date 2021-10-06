import cantools
import can
from tqdm import tqdm
import pandas as pd
import numpy as np
from copy import deepcopy


class BlfReader():
    def __init__(self):
        # set dbc
        self.channel_num = 1
        self.dbc_lists = [[]]
        self.dbcs = [cantools.database.Database()]
        self.blf_name = ""
    
    def set_channel_num(self, n):
        n_old = self.channel_num
        self.channel_num = n
        
        if n<n_old:
            self.dbc_lists = self.dbc_lists[:self.channel_num]
            self.dbcs = self.dbcs[:self.channel_num]
        elif n_old<n:
            tmp = self.dbc_lists
            self.dbc_lists = [[] for _ in range(self.channel_num)]
            self.dbc_lists[:n_old] = tmp
            tmp = self.dbcs
            self.dbcs = [cantools.database.Database() for _ in range(self.channel_num)]
            self.dbcs[:n_old] = tmp
        
    def set_dbc(self, channel, path_to_dbcs):
        channel -= 1
        self.dbc_lists[channel] = path_to_dbcs
        self.dbcs[channel] = cantools.database.Database()
        
        for path in path_to_dbcs:
            self.dbcs[channel].add_dbc_file(path)
    
    def set_blf(self, path_to_blf=None):
        
        if path_to_blf!=None:
            self.blf_name = path_to_blf
        self.blf = can.io.blf.BLFReader(self.blf_name)
            
    
    def prepare(self):
        # prepare data container
        self.id_list = []
        self.signals_list = []
        self.messages_dict = []
        for i in range(self.channel_num):
            self.id_list.append([message.frame_id for message in self.dbcs[i].messages])
            
            self.signals_list.append([message.signal_tree for message in self.dbcs[i].messages]) 
        
            ## dictionaryでdataframeに1行づつ追加するのは遅いからやめた
            #self.messages_dict = {id_:pd.DataFrame(index=[], columns=signals)
            #                      for (id_, signals) in zip(self.id_list, self.signals_list)}
            self.messages_dict.append({id_:{signal:[] for signal in signals}
                                       for (id_, signals) in zip(self.id_list[i], self.signals_list[i])})
            for id_ in self.messages_dict[i].keys():
                self.messages_dict[i][id_]["time"] = []
        
        # prepare blf
        self.set_blf()
        
        self.tmp_list = []
        self.messages_dict_ = deepcopy(self.messages_dict)
        
    def delete_gabage(self):
        try:
            del self.messages_dict
            del self.tmp_list
        except:
            pass
    
    def to_df(self, block_length=1000000):
        
        def make_tmp_df():
            
            # make dataframe from lists for each id
            for chan in range(self.channel_num):
                for id_ in self.id_list[chan]:
                    self.messages_dict[chan][id_] = pd.DataFrame(
                        data = {signal:self.messages_dict[chan][id_][signal] 
                                for signal in self.messages_dict[chan][id_].keys()}
                    )
            
            # cancat all df and sort
            tmp = []
            for dict_list in self.messages_dict:
                #tmp += dict_list.values()
                tmp.append(pd.concat(dict_list.values()))
            if len(tmp)>1:
                tmp_df = pd.concat(tmp)
            else:
                tmp_df = tmp[0]
            tmp_df = tmp_df.set_index('time')
            tmp_df = tmp_df.sort_index()
            
            # store sorted df
            self.tmp_list.append(tmp_df)
            
            # make messages_dict to empty
            self.messages_dict = deepcopy(self.messages_dict_)
            
            
        # initialize
        self.prepare()
        
        # load blf
        for i, message in tqdm(enumerate(self.blf)):
            chan = message.channel
            
            if chan<self.channel_num:
                if message.arbitration_id in self.id_list[chan]:
                
                    id_ = message.arbitration_id
                    time = message.timestamp
                    data = message.data
                    
                    decoded_data = self.dbcs[chan].decode_message(id_, data)
                    
                    ## 各信号のリストに追加していき、最後にdatarameに変換する方法
                    ## locでdictionaryから追加する方法の70倍ぐらい早い
                    self.messages_dict[chan][id_]["time"].append(time)
                    for signal in decoded_data.keys():
                        self.messages_dict[chan][id_][signal].append(decoded_data[signal])
                    
                    ## locでdictionaryから追加する方法 平均450µsぐらいかかって相当遅い
                    #self.messages_dict[id_].loc[time] = decoded_data
                    
            # 一定間隔ごとにソートされたdfを作る
            # 長いblfを一気にソートすると絶望的な時間がかかる
            if i>0 and i%block_length==0:
                make_tmp_df()
        
        self.messages = pd.concat(self.tmp_list)
        #self.messages = self.messages.sort_index()
        
        self.delete_gabage()
        
        return self.messages

if __name__()=="main":
    # read blf
    blf_reader = BlfReader()
    # set channel number
    blf_reader.set_channel_num(2)
    # set dbc files
    dbc_list1 = [
        ".tmp.dbc",
    ]
    blf_reader.set_dbc(channel=1, path_to_dbcs=dbc_list1)
    dbc_list2 = [
        ".tmp2.dbc",
    ]
    blf_reader.set_dbc(channel=2, path_to_dbcs=dbc_list2)
    # set blf file
    blf_path = "tmp.blf
    blf_reader.set_blf(blf_path)
    # convert blf to dataframe
    df = blf_reader.to_df()
    