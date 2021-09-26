import pandas as pd
import requests, time
from bs4 import BeautifulSoup
from fredapi import Fred
import conn_db, helper

fred = Fred(helper.fred_api_key)
# 가져올 FRED ID
fred_get_list = conn_db.from_('Master_FRED','fred_codes')['code'].tolist()


def _get_meta_date(index_id):
	'''
	1개 id의 index 정보 가져오기
	'''
	df = pd.DataFrame(fred.get_series_info(index_id)).T
	return df

def _get_fred_data(stat_id):
	'''
	stat_id의 날짜별 값 가져오기
	'''
	df = pd.DataFrame(fred.get_series(stat_id))
	df.index.name = 'dt'  # 날짜열 지정
	df.reset_index(inplace=True)

	df.insert(0, 'IndexID', stat_id)  # id열 추가
	df.rename(columns={0: '값'}, inplace=True)
	# null값 제거
	filt = df['값'].notnull()
	df = df.loc[filt].copy().reset_index(drop=True)
	return df

def update_fred_meta_info():
	info = pd.DataFrame()
	for index_id in fred_get_list:
		try:
			temp = _get_meta_date(index_id)

		except ValueError :
			try:
				time.sleep(2)
				temp = _get_meta_date(index_id)
			except:
				print(f'{index_id} 오류 : ValueError')
				pass
		if len(temp)>0:
			info = info.append(temp, ignore_index=True)
			time.sleep(2)

	drop_cols = ['realtime_start', 'realtime_end', 'frequency', 'seasonal_adjustment_short',
				'popularity', 'notes', 'units_short']

	info.drop(columns=drop_cols,inplace=True)

	info_old = conn_db.from_('Master_FRED', 'import_fred_code_info')
	info = pd.concat([info, info_old], ignore_index=True)
	info = info.drop_duplicates(subset=['id']).reset_index(drop=True)

	# 데이터 저장
	conn_db.to_(info, 'Master_FRED', 'import_fred_code_info')

@helper.timer
def update_fred_data():
	'''
	FRED Data 업데이트 후 저장
	'''

	df = pd.DataFrame()
	for stat_id in fred_get_list:
		# data가져오기
		try:
			data_temp = _get_fred_data(stat_id)

		except ValueError :
			try:
				time.sleep(10)
				data_temp = _get_fred_data(stat_id)
			except :
				print(f'{stat_id} 오류')

		if len(data_temp)>0:
			df = df.append(data_temp, ignore_index=True)
			time.sleep(5)
	# export
	df.reset_index(drop=True, inplace=True)
	if platform.system() =='Windows':
		df.to_pickle(conn_db.get_path('fred_raw')+'.pkl')
	else:
		path = r'/Users/jbl_mac/Downloads/FRED_Data.pkl'
		df.to_pickle(path)

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