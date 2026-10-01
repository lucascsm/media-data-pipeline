import os
import pandas as pd

df=pd.read_parquet(os.environ['DIAGNOSTIC_PARQUET_PATH'])
df1=pd.DataFrame(df)
df2=df1.copy()
print(len(df1.columns))
# print(df1.columns)
# df=df.groupby('nivel')['start_trial_mobile_app'].sum().reset_index()
# df=df[df['nivel']=='anuncio']
# print(df[['id_campanha','id_grupo_anuncio', 'nome_anuncio']])
# ad_name_especifico=df[df['ad_name'].str.contains('DEMO2025', case=False, na=False)]
# print(ad_name_especifico['ad_name'].unique())
# df1=df[df['nivel']=='anuncio']
# df2=df[df['nivel']=='grupo_anuncio']
# print(df2[['id_grupo_anuncio','nome_grupo_anuncio']].head(20))
# cruza = pd.merge(
#     df1,
#     df2[['id_grupo_anuncio', 'nome_grupo_anuncio']],
#     on='id_grupo_anuncio',
#     how='left',
#     suffixes=('', '_df2'))

# cruza = df1.merge(
#     df2[['data','id_grupo_anuncio','nome_grupo_anuncio']],
#     on=['data','id_grupo_anuncio'],  # inclui data para não duplicar linhas
#     how='left',
#     suffixes=('', '_df2'),
# )

# cruza['nome_grupo_anuncio'] = cruza['nome_grupo_anuncio'].fillna(cruza['nome_grupo_anuncio_df2'])
# cruza = cruza.drop(columns=['nome_grupo_anuncio_df2'])
# print(cruza[[ 'nome_anuncio', 'nome_grupo_anuncio']].head(20))