from tabula import read_pdf

file_name = r'C:\Users\09276425\Documents\Tmp\Boxed\POs\PO-0114-01134.pdf'

all_dfs = read_pdf(file_name, pages="all")

num_dfs = len(all_dfs)
print('num_dfs:', num_dfs)

# ########################################
# Get last N df (contain items list) & process

table_num = 2
table_num = 1 #!!!!!!!!!!!!!!
if (num_dfs == 2):
    table_num = 1
while (table_num < num_dfs):
    print('table_num:', table_num)
    df = all_dfs[table_num]
    #print(df)
    #exit(1)
    #df.drop(axis=1, columns='Unnamed: 9', inplace=True)

    num_cols = len(df.columns)
    # print('num_cols:', num_cols)
    cols = ['GID', 'SKU', 'Name', 'Description', 'UOM', 'Order_Qty', 'Cost_Per_UOM', 'Total_Cost']
    max_cols = len(cols)
    # print('max_cols:', max_cols)

    # Drop cols til we get to desired #
    while (num_cols > max_cols):
        col_name = df.columns[num_cols-1]
        df.drop(axis=1, labels=col_name, inplace=True)
        #print('new num_cols:', len(df.columns))
        num_cols -= 1

    df.columns = cols

    # Drop old col header
    df.drop(axis=0, labels=0, inplace=True)

    print(df)
    print(df.iloc[3])
    table_num += 1
    #exit(1)


