

import time
import requests
import pandas as pd
from io import StringIO
import datetime





def keggGet(url):

    max_retries = 10
    retry_delay = 2
    retries = 0
    while retries < max_retries:
        try:
            #1 call KEGG API
            response = requests.get(url)
            response.raise_for_status()
            KEGG_result = response.text
            break  # 调用成功则跳出重试循环
        except requests.RequestException as e:
            #logging.error(f"getsFromEntry:{entry_id}, 第{retries + 1}次调用SOAP服务出现错误:  {e}")
            print(f"第{retries + 1}次调用SOAP服务出现错误: {e}")
            retries += 1
            if retries < max_retries:
                print(f"等待 {retry_delay} 秒后进行下一次重试...")
                time.sleep(retry_delay)
            else:
                print(f"失败，达到最大重试次数，放弃重试。")


    return KEGG_result




def keggGetLoop(entry_ids,ko_data_frame,failed_position = 0):
    '''

    :param entry_ids:
    :return:
    '''

    df4 = ko_data_frame
    for i in range(len(entry_ids)):
        print(len(entry_ids))
        print(i)
        entry_id = entry_ids[i]
        print(entry_id)

        if i >= failed_position and entry_id:
            try:
                # 1  call KEGG_API query
                url = f'https://rest.kegg.jp/link/ko/{entry_id}'
                kegg_result = keggGet(url)
                df2 = pd.read_csv(StringIO(kegg_result), sep='\t')

                # 2  拆分表格
                df3 = df2
                df3.columns = [f"{entry_id}", "ko"]
                df3[f"{entry_id}"] = df3[f"{entry_id}"].str.split(":").str[1]
                df3["ko"] = df3["ko"].str.split(":").str[1]
                #df3.to_csv(output_file,index=False)

                # 3  查询有无

                df3 = df3[df3.columns[1:]]
                df3 = df3.drop_duplicates(keep='last')  # 去冗余
                df3.columns = [f"{entry_id}"]

                df4 = pd.merge(df4, df3, left_on="KO", right_on=f"{entry_id}", how="left")
                df4[f"{entry_id}"] = df4[f"{entry_id}"].apply(lambda x: 1 if pd.notnull(x) else 0)
                df4.to_csv("KODistribution_AllOrganism.csv")
                # 4  记录
                with open('log.txt', 'a', encoding='utf-8') as file:
                    # 获取当前时间
                    #current_time = datetime.datetime.now()
                    file.write(f"{i},{entry_id},{current_time},success\n")

            except Exception as e:
                #logging.error(f"{i},{entry_id}: getsFromEntry 时出现错误: {e}，跳过当前，继续下一次循环")
                print(f"{i},{entry_id}: keggGetLoop 时出现错误: {e}，跳过当前，继续下一次循环")
                with open('log_error.txt', 'a', encoding='utf-8') as file:
                    # 获取当前时间
                    current_time = datetime.datetime.now()
                    file.write(f"{i},{entry_id}: {current_time}\n,keggGetLoop 时出现错误: {e}，跳过当前，继续下一次循环\n")
    return df4







if __name__ == '__main__':

    #1 all organism
    url = 'https://rest.kegg.jp/list/organism'
    #output_file = "organism.txt"
    data1 = keggGet(url)
    #column_names = ["Organism_ID","Abbreviation","Name","Taxonomy"]
    df1 = pd.read_csv(StringIO(data1), sep='\t',header=None,names=["Organism_ID","Abbreviation","Name","Taxonomy"])
    df1.to_csv("organism.csv",index=False,header=True)
    #2
    url = 'https://rest.kegg.jp/list/ko'
    data2 = keggGet(url)
    df2 = pd.read_csv(StringIO(data2), sep='\t',header=None,names=["KO","Description"])
    df2.to_csv("ko.csv", index=False, header=True)


    entry_ids = df1.iloc[:,1]
    #entry_ids = sorted(entry_ids)
    df3 = keggGetLoop(entry_ids,df2,0)
    #df3.to_csv("KODistribution_AllOrganism.csv")


