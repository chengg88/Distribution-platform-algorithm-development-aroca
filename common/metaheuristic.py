import numpy as np
import pandas as pd
import random
import itertools
from sklearn.preprocessing import normalize
from collections import Counter
import sys
sys.path.append('../config')
from config import global_var

class GA:
    def __init__(self, suitable_deli):
        global_var.set_value('suitable_deli', suitable_deli)

    ### 比較change rate並從可行解中隨機更換每個位置的基因，以產生新的染色體組合，每次一條
    def rand_gene(self, initial, chromo_len, changerate, possible_ans): 
        rand_chromo = []
        change_set = [random.uniform(0, 1) for _ in range(chromo_len)] #亂數設定突變率
        for chromo_sit in range(chromo_len): 
            if change_set[chromo_sit] < changerate: #比較change rate，小於則改變基因
                new_gene = possible_ans[chromo_sit][random.randint(0, len(possible_ans[chromo_sit])-1)] #從可行解中隨機更換該位置的基因
                rand_chromo.append(new_gene)
            else: #未小於則使用initial基因
                new_gene = initial[chromo_sit]
                rand_chromo.append(new_gene)
        return rand_chromo
    
    ### 檢查每一條染色體是否符合限制
    def constrain_check(self, delivery_list, DISTR_INFO, upper_bound, lower_bound, check_pla): 
        deli_portion = {}
        deli_type = []
        low_minus, up_minus = 0, 0

        for element in delivery_list:
            each_deli_type = DISTR_INFO.loc[DISTR_INFO['DISTR_TYPE_ID'] == element , ['DISTR_ID']].values[0].tolist()
            deli_type.append(each_deli_type)
        deli_type = sum(deli_type, []) #2d list to 1d
        for type_element in deli_type:
            deli_portion[type_element] = deli_type.count(type_element)
        for dis in DISTR_INFO['DISTR_ID'].unique().tolist():
            if dis not in deli_portion.keys():
                deli_portion[dis] = 0
        # constrain_check for initial
        if check_pla == 0:
            for deli_element in deli_portion.keys(): #檢查每一條染色體是否有符合每一家配送商的上限    
                if lower_bound[deli_element] > deli_portion[deli_element]: #若小於下界就中斷，重新生成
                    delivery_list = None
                    break
                if upper_bound[deli_element] < deli_portion[deli_element]: #若大於上界就中斷，重新生成
                    delivery_list = None
                    break
            return delivery_list
        else:
            for deli_element in deli_portion.keys(): #檢查每一條染色體是否有符合每一家配送商的上限    
                if lower_bound[deli_element] > deli_portion[deli_element]: #若小於下界就加上懲罰
                    low_minus = abs(lower_bound[deli_element] - deli_portion[deli_element]) + low_minus
                if upper_bound[deli_element] < deli_portion[deli_element]: #若大於上界就加上懲罰
                    up_minus = abs(upper_bound[deli_element] - deli_portion[deli_element]) + up_minus
            fitness_punish = np.sum(low_minus, up_minus)*-100
            return fitness_punish

                            
    ### 計算每條染色體的fitness
    def get_fitness(self, every_chromos, DISTR_INFO, box_num, port_f, port_t, port_q, fee_list, pack_vol, chromo_len, times_list, quality_list, delivery_id): 
        # 依照不同的染色體基因(配送商)取得整批訂單的運費評分
        each_fitness = 0
        order_fee = []
        for chromo_ind in range(len(every_chromos)):
            for vol_ind in range(len(pack_vol[chromo_ind])):
                aorder_fee = [fee_list[str(pack_vol[chromo_ind][vol_ind])][every_chromos[chromo_ind]]]
                order_fee.append(aorder_fee)
        each_fitness = port_f * sum(np.multiply(sum(order_fee,[]) ,sum(box_num,[]))) #list(正規化後的運費與1d箱數)相乘

        ### 品質、時效評分正規化(0~1)
        quality_sc = (normalize(quality_list) + 0.01).flatten().tolist()
        quality_dict = dict(zip(delivery_id, quality_sc))
        quality_type_sc = dict()
        for element in quality_dict.keys():
            each_quality_sc = np.unique(DISTR_INFO.loc[DISTR_INFO['DISTR_ID'] == element , ['DISTR_TYPE_ID']].values.flatten()).tolist()
            for ind in each_quality_sc:
                quality_type_sc[ind] = quality_dict[element]

        times_sc = (normalize(times_list) + 0.01).flatten().tolist()
        times_dict = dict(zip(delivery_id, times_sc))
        times_type_sc = dict()
        for element in times_dict.keys():
            each_times_sc = np.unique(DISTR_INFO.loc[DISTR_INFO['DISTR_ID'] == element , ['DISTR_TYPE_ID']].values.flatten()).tolist()
            for ind in each_times_sc:
                times_type_sc[ind] = times_dict[element]

        for each_gene in every_chromos:
            quality = quality_type_sc[each_gene] #取得訂單的品質分數
            times = times_type_sc[each_gene]
            gene_fitness = port_t * times + port_q * quality #一條染色體中各自基因的fitness
            each_fitness += gene_fitness

        return each_fitness
    
    ### 計算每條染色體fitness佔全體的比例    
    def cal_fitness(self, all_fitness): 
        fitness_cumulate = []
        sum_fitness = sum(all_fitness) #fitness總和
        for each_fitness in range(len(all_fitness)):
            fitness_portion = abs(all_fitness[each_fitness] / sum_fitness) #計算各個fitness佔總合的比例
            if each_fitness == 0: #第一條染色體不需要累加
                fitness_cumulate.append(fitness_portion)
            else: #其餘的需要累加上一筆
                fitness_cumulate.append(fitness_portion + fitness_cumulate[each_fitness-1])
        return fitness_cumulate
    
    ### 輪盤法選擇要交配的兩條染色體
    def selection(self,fitness_select, all_chromo): 
        check = 0
        pick_chromoset = []
        pick_place = [random.uniform(0, 1) for _ in range(2)] #輪盤法在0到1間挑兩個值
        for one_place in pick_place:
            pick_chromo = all_chromo[np.where(np.array(fitness_select) >= one_place)[0][0]] #挑出來的值對應的位置對應的染色體
            pick_chromoset.append(pick_chromo)
        return pick_chromoset
    
    ### 將兩條染色體進行交配
    def crossover(self, chromosome_list, chromo_len): 
        CR = 0.9 #交配率
        cross_ornot = random.uniform(0,1) #設定交配參數
        if cross_ornot < CR:  #小於交配率就交配
            cross_place = random.randint(0, chromo_len-1) #隨機選定要交配的位置
            new_chromo1 = chromosome_list[0][0:cross_place+1] + chromosome_list[1][cross_place+1:] #+1避免選到[0:0]時沒取到任何值
            new_chromo2 = chromosome_list[1][0:cross_place+1] + chromosome_list[0][cross_place+1:]
        else: #大於就保持原樣
            new_chromo1 = chromosome_list[0]
            new_chromo2 = chromosome_list[1]
        return new_chromo1, new_chromo2
    
    ### 將兩條染色體進行突變
    def mutation(self, chromosome_list, possible_ans): 
        MR = 0.2 #突變率
        muta_gene = [] ## 可行解大於一個的地方才能突變 
        element_nums = np.array(np.where([len(possible_ans[ind]) > 1 for ind in range(len(possible_ans))])[0].tolist())
        for find_plac in range(len(element_nums)): # 依照位置取得該些可行解
            muta_gene.append(possible_ans[element_nums[find_plac]])  
        muta_ornot = random.uniform(0,1) # 突變參數
        if muta_ornot < MR:  #小於突變率就突變
            #處理第一條突變(new_chromos[0])
            muta_place1 = random.randint(0, len(muta_gene)-1) #隨機選定要突變的位置(頭跟尾也可以突變)
            muta_candiate1 = muta_gene[muta_place1].copy() #查看選定的突變位置對應的那張訂單的可行解有哪些
            pick_mutagene1 = muta_candiate1[random.randint(0,len(muta_candiate1)-1)] #選擇要用哪個可行解來當新基因
            if pick_mutagene1 == chromosome_list[0][muta_place1]: #如果挑到的新基因等於舊基因就重新選擇
                muta_candiate1.remove(pick_mutagene1) #將該基因從該訂單可行解list中刪除
                picknew_mutagene1 = muta_candiate1[random.choice(list(range(0,len(muta_candiate1))))] #從剩下的可行解隨機挑選一個
                updated_chromo1 = chromosome_list[0][:muta_place1] + [picknew_mutagene1] + chromosome_list[0][muta_place1+1:] #組合成新染色體
            else: #如果挑到的新基因不等於舊基因就存成新染色體
                updated_chromo1 = chromosome_list[0][:muta_place1] + [pick_mutagene1] + chromosome_list[0][muta_place1+1:] #組合成新染色體
            #處理第二條突變(new_chromos[1])
            muta_place2 = random.randint(0, len(muta_gene)-1) #隨機選定要突變的位置(頭跟尾也可以突變)
            muta_candiate2 = muta_gene[muta_place2].copy() #查看選定的突變位置對應的那張訂單的可行解有哪些
            pick_mutagene2 = muta_candiate2[random.randint(0,len(muta_candiate2)-1)] #選擇要用哪個可行解來當新基因
            if pick_mutagene2 == chromosome_list[1][muta_place2]: #如果挑到的新基因等於舊基因就重新選擇
                muta_candiate2.remove(pick_mutagene2)
                picknew_mutagene2 = muta_candiate2[random.choice(list(range(0,len(muta_candiate2))))] #從剩下的可行解隨機挑選一個
                updated_chromo2 = chromosome_list[1][:muta_place2] + [picknew_mutagene2] + chromosome_list[1][muta_place2+1:] #組合成新染色體
            else: #如果挑到的新基因不等於舊基因就存成新染色體
                updated_chromo2 = chromosome_list[1][:muta_place2] + [pick_mutagene2] + chromosome_list[1][muta_place2+1:] #組合成新染色體
        else: #大於就保持原樣不突變
            updated_chromo1 = chromosome_list[0]
            updated_chromo2 = chromosome_list[1]
        return updated_chromo1, updated_chromo2

    def execute(self, best_deli, possible_ans, DISTR_INFO, box_num, upper_bound, lower_bound, port_f, port_t, port_q, fee_list, ori_vol, ori_times, ori_quality, ID, GA_cfg):
        global suitable_deli
        pop_size = GA_cfg['pop_size']
        epoch = GA_cfg['epoch']
        iteration = GA_cfg['iteration']
        chromo_len = len(best_deli)

        best_fitness = []
        for epoch_time in range(epoch):
            all_chromo = []
            if epoch_time == 0: # 生資料直到滿足pop_size(第一次迭代才需要)
                changerate = 0.5
                check_pla = 0
                all_chromo.append(best_deli)
                while len(all_chromo) != pop_size:
                    rand_inichromo = self.rand_gene(best_deli, chromo_len, changerate, possible_ans)
                    chromosome = self.constrain_check(rand_inichromo, DISTR_INFO, upper_bound, lower_bound, check_pla)
                    if chromosome is not None:
                        all_chromo.append(chromosome)
            else: #後續迭代
                all_chromo = best100_chromo.copy() #後續迭代用母代+子代的最佳100個染色體做初始
            
            # 算fitness
            paraent_fitness = []
            for each_chromo in all_chromo: #為all_chromo裡的每一條染色體算fitness
                paraent_fitness.append(self.get_fitness(each_chromo, DISTR_INFO, box_num, port_f, port_t, port_q, fee_list, ori_vol, chromo_len, ori_times, ori_quality, ID))
            fitness_cumulate = self.cal_fitness(paraent_fitness) #計算每條染色體fitness佔全體比例
            
            # 交配與突變
            kid_chromos = []
            fitness_punish = []
            check_pla = 1
            for iter_time in range(iteration): 
                select_chromo = self.selection(fitness_cumulate, all_chromo) #輪盤法挑選兩條要交配的染色體
                new_chromos = list(self.crossover(select_chromo, chromo_len)) #交配完後的兩條染色體
                for chromo_list in new_chromos:
                    fitness_punish.append(self.constrain_check(chromo_list, DISTR_INFO, upper_bound, lower_bound, check_pla)) #交配後檢查比例，若不符合則加上懲罰值
                kid_chromos.append(list(self.mutation(new_chromos, possible_ans)))#突變完後的兩條染色體
            kid_chromos = list(itertools.chain.from_iterable(kid_chromos)) #讓突變完後的染色體變成2d list形式
            
            #菁英策略
            ori_kid_fitness = []
            for each_kidchromo in kid_chromos: #計算各子代染色體fitness
                ori_kid_fitness.append(self.get_fitness(each_kidchromo, DISTR_INFO, box_num, port_f, port_t, port_q, fee_list, ori_vol, chromo_len, ori_times, ori_quality, ID))
            kid_fitness = list(np.add(ori_kid_fitness, fitness_punish))
            par_kid_chromo = all_chromo + kid_chromos #將母代與子代染色體放在一起
            par_kid_fitness = paraent_fitness + kid_fitness #將母代與子代fitness放在一起
            par_kid_fitness_sor = np.argsort(par_kid_fitness) #將母帶與子代染色體放在一起並排序(小到大)
            best1 = np.array(par_kid_fitness)[par_kid_fitness_sor[-1:]] #取最大的index(因為是小到大所以要取最後1個)
            print("epoch=",epoch_time,"best fitness in this epoch",best1)
            best_fitness.append(best1)
            best100 = par_kid_fitness_sor[-100:] #取前一百個大的index(因為是小到大所以要取最後100個)
            best100_chromo = []
            for chromo_in_best100 in best100:
                best100_chromo.append(par_kid_chromo[chromo_in_best100]) #前一百個大的染色體
            suitable_deli = np.array(par_kid_chromo)[par_kid_fitness_sor[-1:]]
            global_var.set_value('suitable_deli', suitable_deli.flatten().tolist())