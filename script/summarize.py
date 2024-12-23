import pandas as pd
from param import DataFrame


def process_tables(table1):


    # 初始化表2
    table2 = pd.DataFrame()
    module_numbers = table1.loc[:, "transportModule"].unique()

    for i in range(len(module_numbers)):
        print(i)
        module = module_numbers[i]  # .strip()
        print(module)

        module_data = table1[table1["transportModule"] == module]
        rule = module_data["rule"].iloc[0]
        gene_count = len(module_data)

        #
        organism_index = module_data.columns.get_indexer(['sce'])[0]
        module_data_sub = module_data.iloc[:, organism_index:]
        row_sum = module_data_sub.sum()

        if rule == "and":
            row_sum = row_sum.apply(lambda x: 0 if x < gene_count else (1 if x == gene_count else x))
            module_data_sub_sum = pd.DataFrame([row_sum])
            module_data_sub_sum.insert(0, 'Module', f'{module}')
            module_data_sub_sum.insert(1, 'rule', 'or')
            module_data_sub_sum.insert(2, 'module_KO_count', f'{gene_count}')
        else:
            row_sum = row_sum.apply(lambda x: 1 if x > 0 else x)
            module_data_sub_sum = pd.DataFrame([row_sum])
            module_data_sub_sum.insert(0, 'Module', f'{module}')
            module_data_sub_sum.insert(1, 'rule', 'or')
            module_data_sub_sum.insert(2, 'module_KO_count', f'{gene_count}')

        # print(module_data_sub_sum)
        table2 = table2._append(module_data_sub_sum, ignore_index=True)

    return table2

def process_tables_2(table1):

    module_data = table1
    gene_count = len(module_data)

    #
    organism_index = module_data.columns.get_indexer(['sce'])[0]
    module_data_sub = module_data.iloc[:, organism_index:]


    row_sum = module_data_sub.sum()
    row_sum = row_sum.apply(lambda x: 1 if x > 0 else x)
    module_data_sub_sum = pd.DataFrame([row_sum])
    module_data_sub_sum.insert(0, 'Module', 'transport')
    module_data_sub_sum.insert(1, 'rule', 'or')
    module_data_sub_sum.insert(2, 'module_count', f'{gene_count}')


    table2 = module_data_sub_sum

    return table2


def sum_table3(table1):
    module_data = table1
    table4 = pd.DataFrame({"Module":"transport","module_count":table1["module_count"]})

    organism_index = module_data.columns.get_indexer(['sce'])[0]
    module_data_sub = module_data.iloc[:, organism_index:]
    row_sums = module_data_sub.sum(axis=1)
    row_sum = row_sums.iloc[0]
    ratios = row_sum/len(module_data_sub.columns)
    table4.insert(2,'sum',f'{row_sum}')
    table4.insert(3, 'total', f'{len(module_data_sub.columns)}')
    table4.insert(4, 'ratios', f'{ratios}')


    return table4

# 假设你的表1数据存储在一个名为data.csv的文件中
excel_file = 'target_ko.xlsx'

# 遍历Excel文件中的所有子表
xls = pd.ExcelFile(excel_file)
sheet_names = xls.sheet_names

for i in range(len(sheet_names)):
    if i > 0:
        print(i)
        sheet_name = sheet_names[i]
        print(sheet_name)
        table1 = pd.read_csv(f"result1/{sheet_name}.csv")

        table2 = process_tables(table1)
        table2.to_csv(f"result2/{sheet_name}.csv")

        table3 = process_tables_2(table2)
        table3.to_csv(f"result3/{sheet_name}.csv")

        table4 = sum_table3(table3)
        table4.to_csv(f"result4/{sheet_name}_summarize.csv")
