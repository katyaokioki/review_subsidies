import pandas as pd

df1 = pd.read_excel('downloads/Реестр отборов.xlsx')
df2 = pd.read_excel('downloads/Реестр отборов1.xlsx')


only_excel = df1.merge(df2[["Шифр отбора"]].drop_duplicates(), on="Шифр отбора", how="left", indicator=True)


only_excel = only_excel.loc[only_excel["_merge"] == "left_only"].drop(columns=["_merge"])
print(only_excel)
