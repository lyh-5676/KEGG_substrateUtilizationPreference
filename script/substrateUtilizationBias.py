



import pandas as pd


if __name__ == "__main__":

    # 假设df_test是你想要合并的测试数据框
    df_test = pd.read_csv("KODistribution_AllOrganism_subset.csv")
    df_organism_selected = pd.read_excel("organism.xlsx",sheet_name="1.bac+fungi_9125")
    column_id_list1 = ["KO","Description"]
    column_id_list2 = df_organism_selected.iloc[:,1]

    column_id_list = []
    column_id_list.extend(column_id_list1)
    column_id_list.extend(column_id_list2)
    #column_id_list = list(set(column_id_list))
    df_test_selected = df_test[column_id_list]

    # 读取Excel文件
    excel_file = 'target_ko.xlsx'

    # 遍历Excel文件中的所有子表
    xls = pd.ExcelFile(excel_file)
    sheet_names = xls.sheet_names

    for i in range(len(sheet_names)):

        # 读取当前子表
        print(i)
        sheet_name = sheet_names[i]

        if i > 0 and sheet_name:
            df = pd.read_excel(xls, sheet_name)

            # 合并当前子表与df_test，假设它们有一个共同的列'Key'
            # 如果列名不同，你需要相应地调整合并的键
            merged_df = pd.merge(df, df_test_selected, left_on='KOnumber',right_on='KO', how='left')

            # 输出为新的CSV文件，文件名按照子表的名称来命名
            output_file = f"{sheet_name}.csv"
            merged_df.to_csv(f"{output_file}", index=False)
            print(f"Saved {output_file}")





