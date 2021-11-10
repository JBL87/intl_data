import pandas as pd
import conn_db, helper

# 가져올 FRED ID
fred_get_list = conn_db.from_('Master_FRED','fred_codes')['code'].tolist()

@helper.timer
def join_fred_data_and_meta():
	'''
	FRED 데이터와 META 정보 df를 join한 다음 저장
	'''
	# join할 df 불러오기
	if platform.system() =='Windows':
		file = conn_db.get_path('fred_raw')+'.pkl'
	else:
		file = r'/Users/jbl_mac/Downloads/FRED_Data.pkl'
	df = pd.read_pickle(file)

	meta_df = conn_db.from_('Master_FRED', 'FRED_Meta')
	meta_df = meta_df.rename(columns={'id': 'IndexID'})
	filt = meta_df['IndexID'].notna()
	meta_df = meta_df.loc[filt].copy().reset_index(drop=True)

	meta_df['데이터기간'] = meta_df['observation_start'] + '~' + meta_df['observation_end']

	cols = ['순번', 'observation_start', 'observation_end',
         'frequency_short', 'seasonal_adjustment']
	meta_df.drop(columns=cols, inplace=True)

	#합치기---------------------
	df = df.merge(meta_df, how='inner', on='IndexID')
	df.rename(columns={'dt': '날짜'}, inplace=True)

	#index컬럼---------------------
	index_cols = meta_df.columns.tolist()
	index_cols.remove('통계명(한글)')
	index_cols.append('날짜')

	#unpivot--------
	df = df.pivot_table(index=index_cols, columns='통계명(한글)',
	                    values='값').reset_index()
	df.columns.name = None
	# 저장
	if platform.system() =='Windows':
		conn_db.export_(df, 'fred_final')
	else:
		file = r'/Users/jbl_mac/Downloads/fred_final.pkl'
		df.to_pickle(file)