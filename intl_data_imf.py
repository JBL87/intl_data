import platform, sys
if platform.system() =='Windows':
    sys.path.append(r"C:\Users\bong2\OneDrive\Python_Codes\00_python_scripts") # script 경로 window용
else:
    sys.path.append(r'/Users/jbl_mac/OneDrive/Python_Codes/00_python_scripts') # script 경로 mac용

from glob import glob
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup

from fredapi import Fred
import conn_db
import helper

fred = Fred(helper.fred_api_key)
# 가져올 FRED ID
fred_get_list = conn_db.from_('Master_FRED','fred_codes')['code'].tolist()

@helper.timer
def update_fred_meta_info():

	def get_meta_date(index_id):
		'''
		1개 id의 index 정보 가져오기
		'''
		df = pd.DataFrame(fred.get_series_info(index_id)).T
		return df

	#-------------------------------------------------
	info = pd.DataFrame()
	for index_id in fred_get_list:
		try:
			temp = get_meta_date(index_id)

		except ValueError :
			try:
				time.sleep(2)
				temp = get_meta_date(index_id)
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
	def get_fred_data(stat_id): 
		'''
		stat_id의 날짜별 값 가져오기
		'''
		df = pd.DataFrame(fred.get_series(stat_id))
		df.index.name = 'dt'# 날짜열 지정
		df.reset_index(inplace=True)

		df.insert(0, 'IndexID', stat_id)  # id열 추가
		df.rename(columns={0: '값'}, inplace=True)
		# null값 제거
		filt = df['값'].notnull()
		df = df.loc[filt].copy().reset_index(drop=True)
		return df

	#-------------------------------------------------
	df = pd.DataFrame()
	for stat_id in fred_get_list:
		# data가져오기
		try:
			data_temp = get_fred_data(stat_id)

		except ValueError :
			try:
				time.sleep(10)
				data_temp = get_fred_data(stat_id)
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

@helper.timer
def get_data_from_investings():
	print('investings에서 데이터 가져오기 시작')
	user_agent = helper.user_agent
	#---------------------------------------------------------------------------
	def make_df_from_investings(url):
		r = requests.get(url, headers={'User-Agent': user_agent})
		dom = BeautifulSoup(r.text, "html.parser")
		headers = [x.text for x in dom.select(
			'#results_box > table > thead > tr > th')]
		datas = [x.text for x in dom.select(
			'#results_box > table > tbody > tr > td')]
		df = pd.DataFrame()
		for i in range(len(headers)):
			temp_df = pd.DataFrame(datas[i::len(headers)])
			df = pd.concat([df, temp_df], axis=1)
		df.columns = headers
		return df.dropna()
	#---------------------------------------------------------------------------
	investings_dict = {'https://kr.investing.com/indices/phlx-semiconductor-historical-data': '필라델피아 반도체지수',
						'https://kr.investing.com/indices/usdollar-historical-data': 'Dollar지수',
						'https://kr.investing.com/currencies/usd-krw-historical-data': 'USD_KRW',
						'https://kr.investing.com/commodities/crude-oil-historical-data': 'WTI유',
						'https://kr.investing.com/indices/msci-world-historical-data': 'MSCI WORLD',
						'https://kr.investing.com/indices/msci-em-historical-data': 'MSCI EM'}
	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_Investings\1_다운받은 원본\\"
	cols = ['날짜', '종가']
	df_all = pd.DataFrame()
	#------------------ ------------------ ------------------ ------------------
	for url in investings_dict.keys():
		new = make_df_from_investings(url)
		name = investings_dict[url]
		filename = folder + f'{name}_취합본.pkl'
		old = pd.read_pickle(filename)
		df = pd.concat([new, old]).drop_duplicates(subset='날짜')
		df = df.sort_values(by=['날짜'], ascending=False).reset_index(drop=True)
		df.to_pickle(filename)

		df = df[cols].rename(columns={'종가': name})
		df_all = df_all.append(df)
	#------------------ ------------------ ------------------ ------------------
	for col in df_all.columns.tolist()[1:]:
		if df_all[col].str.contains(',').sum() > 0:
			df_all[col] = df_all[col].str.replace(',', '')
		df_all[col] = df_all[col].astype('float')
	df_all = df_all.groupby('날짜').agg('mean').reset_index()
	df_all = df_all.sort_values(by=['날짜'], ascending=False).reset_index(drop=True)
	#------------------ ------------------ ------------------ ------------------
	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_Investings\0_hyper\\"
	df_all.to_pickle(folder + 'investings_취합본.pkl')

	print('investings에서 데이터 가져오기 완료')
	#--------------------------------------------------------------------------------------------------------------------

@helper.timer
def imf_pcps():  # IMF comodity_price_index
    # metadata 불러와서 정리
	print('IMF comodity_price_index 정리 시작')
	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_IMF\1_Primary_Comodity_Price_System\\"

	file = folder + "Metadata_PCPS.csv"
	df_map = pd.read_csv(file, encoding='utf-8')
	filt = df_map['Metadata Attribute'] == 'Commodity Definition'
	df_map = df_map.loc[filt, ['Commodity Name',
                            'Commodity Code', 'Metadata Value']]
	df_map = df_map.reset_index(drop=True).drop_duplicates()

	# 데이터 테이블 정리
	file = folder + "PCPS_timeSeries.csv"
	df = pd.read_csv(file, encoding='utf-8')
	dcols = df.columns.tolist()[:7]
	df = df.melt(id_vars=dcols, var_name='날짜',
              value_name='값').dropna(subset=['값'])
    # index랑 달러가격기준만 남기기
	filt = (df['Unit Name'] == 'Index') | (df['Unit Name'] == 'US Dollars')
	df = df.loc[filt].copy().reset_index(drop=True)
    # 필요없는 컬럼삭제하고 날짜 컬럼 값 수정
	dcols = ['Country Name', 'Country Code',
          'Commodity Name', 'Unit Code', 'Attribute']
	df.drop(columns=dcols, inplace=True)
	df['날짜'] = df['날짜'].str.replace('M', '-')
	# 데이터랑 metadata 합치기
	df = df.merge(df_map, on='Commodity Code', how='left')
	dcols = ['날짜', 'Commodity Name', 'Metadata Value']
	df = df.pivot_table(index=dcols, columns='Unit Name',
                     values='값').reset_index()
	df.columns.name = None
	df.rename(columns={'Commodity Name': '상품명',
                    'Metadata Value': '비고'}, inplace=True)
    #------------------------------------------------
    # 저장
	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_IMF\0. hyper_pickle파일\\"
	file_name = 'comodity_price_index'
	df.to_pickle(folder+file_name+'.pkl')

	print('IMF comodity_price_index 정리 완료')
	#--------------------- --------------------- --------------------- --------------------- ---------------------

@helper.timer
def clean_world_bank_comodity():  # world bank comodity 월별 데이터 전처리
	file = r"C:\Users\bong2\OneDrive\DataArchive\DB_World Bank\World Bank_Comodity\DataMonthly\CMOHistoricalDataMonthly.xlsx"
	df = pd.read_excel(file, skiprows=4, encoding='utf-8',
	                   sheet_name='Monthly Prices')
	#------------------------------------------------
	unit = df.iloc[0, ].tolist()[1:]
	code = df.iloc[1, ].tolist()[1:]
	cols = df.columns.tolist()[1:]
	new_cols = []
	for x, y, z in zip(cols, unit, code):
		new_cols.append(x + ' ' + y + '-' + z)
	#------------------------------------------------
	new_cols = ['날짜'] + new_cols
	df.columns = new_cols
	df = df.iloc[2:, ].copy()
	drop_cols = ['COAL_COL', 'COPRA', 'iSTL_JP_INDX', 'STL_JP_CROLL', 'STL_JP_HROLL', 'STL_JP_REBAR',
              'STL_JP_WIROD', 'COTTON_MEMPH', 'RUBBER1_US', 'WHEAT_CANADI', 'WOODPULP']
	for drop_col in drop_cols:
		for col in df.columns.tolist():
			if drop_col in col:
				df.drop(columns=col, inplace=True)
	#------------------------------------------------
	df['날짜'] = df['날짜'].str.replace('M', '-')
	df = df.melt(id_vars='날짜', var_name='item', value_name='값')
	df['item'] = df['item'].str.split('-', expand=True)[0]
	df['item'] = df['item'].str.replace('  ', ' ').str.strip()
	filt = df['값'] == '..'
	df = df.loc[~filt].copy().reset_index(drop=True)
	df['값'] = df['값'].astype('float')
	df = df.pivot_table(index='날짜', columns='item', values='값').reset_index()
	df.columns.name = None
	#------------------------------------------------

	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_World Bank\World Bank_Comodity\0_hyper_pickle\\"
	df.to_pickle(folder + 'worldbank_comodity_취합본.pkl')

@helper.timer
def clean_world_bank_global_economic_monitor():  # World Bank_Global Economic Monitor
	# Master 업로드
	files = glob(conn_db.get_path('world_bank_global_economic_monitor_raw')+'*.xlsx')
	temp = []
	for file in files:
		temp.append(file.split('Monitor\\')[-1].split('.xlsx')[0])
	temp = pd.DataFrame(temp)
	temp.rename(columns={0: '원본'}, inplace=True)
	temp = pd.concat([temp, temp['원본'].str.split(',', expand=True)], axis=1)
	conn_db.to_(temp, 'Master_World Bank', 'Global Economic Monitor_import')

	# 취합 --------------------------------------------------------------
	cols = ['원본', '계절조정', '사용여부', '구분', '통계명']
	map_df = conn_db.from_('Master_World Bank', 'Global Economic Monitor')[cols]
	map_df = map_df[map_df['사용여부'] == '사용'].copy()
	map_df = map_df.reset_index(drop=True).drop(columns='사용여부')
	quarter_dict = {'Q1':'-03-31',
					'Q2': '-06-30',
					'Q3': '-09-30',
					'Q4': '-12-31'}

	files = glob(conn_db.get_path('world_bank_global_economic_monitor_raw')+'*.xlsx')
	df_all = pd.DataFrame()
	for file in files:
		temp_df = pd.ExcelFile(file)
		name = file.split('Monitor\\')[-1].split('.xlsx')[0]
		if name in map_df['원본'].tolist():
			df = pd.DataFrame()
			for freq in temp_df.sheet_names:
				temp = temp_df.parse(sheet_name=freq)
				temp.rename(columns={'Unnamed: 0': '날짜'}, inplace=True)
				temp = temp[temp['날짜'].notna()].copy()
				temp['주기'] = freq
				temp['원본'] = name
				temp = temp.merge(map_df, left_on='원본', right_on='원본', how='inner')
				cols = ['날짜', '주기', '통계명', '원본', '계절조정', '구분']
				temp = temp.melt(id_vars=cols, var_name='국가명', value_name='value')
				if freq == 'monthly':
					temp['날짜'] = temp['날짜'].str.replace('M', '-')
				elif freq == 'quarterly':
					temp['날짜'] = temp['날짜'].str[:4] + temp['날짜'].str[-2:].map(quarter_dict)
				else:
					temp['날짜'] = temp['날짜'].astype(str)
					temp['날짜'] = temp['날짜'].str[:4] + '-01-01'
				df = df.append(temp.dropna(), ignore_index=True)
		df_all = df_all.append(df, ignore_index=True)

	conn_db.export_(df_all, 'world_bank_global_economic_monitor')

	# 국가명 업로드
	temp = df_all[['국가명']].drop_duplicates()
	conn_db.to_(temp,'Master_World Bank','Global Economic Monitor_국가명_import')

@helper.timer
def wolrld_bank():
	# World Bank_Population-Estimates에 있는 국가명
	cols = ['Country Code','Short Name','Table Name',
	        'Long Name','2-alpha code','Region',
	        'Income Group','WB-2 code',]
	df = pd.read_csv(conn_db.get_path('wolrld_bank_population_raw')+".csv",
						encoding='utf-8', usecols=cols)
	conn_db.to_(df, 'Master_국가명','국가명_import_from_WorldBank')

	# World Bank_World Development Indicators
	# 지표 meta data 구글 업로드
	cols = ['Series Code','Topic','Indicator Name','Unit of measure',
	        'Periodicity','Base Period','Aggregation method','Source']
	df = pd.read_csv(conn_db.get_path('wolrld_bank_wdi_raw')+".csv",
						encoding='utf-8', usecols=cols)
	df[['주제','통계명']] = df['Topic'].str.split(':',1,expand=True)
	df.drop(columns=['Topic'], inplace=True)

@helper.timer
def clean_oecd_data():  # OECD 전처리
	oecd_save_folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_OECD\0_hyperfile\\"
	#---------------------------------------------------------------------------

	def country_eng_to_kor(df):  # 국가명 한글화
		cols = ['3자리', '국가명', '소득그룹', '지역', '국가구분']
		temp = conn_db.from_('Master_국가명', '국가명')[cols]
		df = df.merge(temp, left_on='LOCATION', right_on='3자리', how='left')
		df.drop(columns=['3자리', 'LOCATION'], inplace=True)
		del temp
		return df

	def name_eng_to_kor(df):  # 통계명 한글화
		temp = conn_db.from_('Master_OECD', '번역용')[['SUBJECT', '통계명']]
		temp = temp.drop_duplicates().reset_index(drop=True)
		df = df.merge(temp, on='SUBJECT', how='left')
		df.drop(columns=['SUBJECT'], inplace=True)
		del temp
		return df
	#---------------------------------------------------------------------------
	# OECD 경기선행지수----------------------------------------------------------------------------------
	file = r"C:\Users\bong2\OneDrive\DataArchive\DB_OECD\OECD_Composite Leading Indicators\\"
	file = glob(file+'*.csv')
	file.reverse()
	# Master 시트에 업로드
	cols = ['SUBJECT', 'Subject', 'Unit']
	df = pd.read_csv(file[0], encoding='utf-8', usecols=cols)
	df = df.drop_duplicates().reset_index(drop=True)
	conn_db.to_(df, 'Master_OECD', 'MEI_CLI')
	# 사용할 데이터
	cols = ['TIME', 'Value', 'Unit', 'LOCATION', 'SUBJECT', 'Country']
	df = pd.read_csv(file[0], encoding='utf-8', usecols=cols)
	df = df[df['Unit'] != 'Percentage'].copy().reset_index(drop=True)  # 비율은 제외
	# 영문을 한글로 변경
	df = country_eng_to_kor(df)  # 국가명 한글화
	df = name_eng_to_kor(df)  # 통계명 한글화
	df.rename(columns={'TIME': '날짜'}, inplace=True)
	df.drop(columns=['Unit'], inplace=True)
	df['주기'] = '월간'
	df['출처'] = 'OECD'
	# tidy to wide로 변경
	cols = ['날짜', '국가명', 'Country', '소득그룹', '지역', '주기', '출처']
	df = df.pivot_table(index=cols,
                     columns='통계명', values='Value').reset_index()
	df.columns.name = None
	# 파일저장
	df.to_pickle(oecd_save_folder+'OECD_Composite Leading Indicators.pkl')

	# OECD Key Short-Term Economic Indicators---------------------------------------------------------------
	folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_OECD\OECD_Key Short-Term Economic Indicators\\"
	# Master 시트에 업로드
	file = folder + 'KEI_master.csv'
	cols = ['SUBJECT', 'Subject', 'Measure', 'Unit']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	df.sort_values(by=cols, inplace=True)
	df = df.drop_duplicates().reset_index(drop=True)
	conn_db.to_(df, 'Master_OECD', 'Key Short-Term Economic Indicators')
	del df
	# KEI_Production,Sales&Orders
	file = folder + 'KEI_Production,Sales&Orders.csv'
	cols = ['SUBJECT', 'LOCATION', 'TIME', 'Value', 'Country']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	# 영문을 한글로 변경
	df = country_eng_to_kor(df)  # 국가명 한글화
	df = name_eng_to_kor(df)  # 통계명 한글화
	df.rename(columns={'TIME': '날짜'}, inplace=True)
	df['주기'] = '월간'
	df['출처'] = 'OECD'
	cols = ['날짜', '국가명', 'Country', '소득그룹', '지역', '주기', '출처']
	df = df.pivot_table(index=cols, columns='통계명', values='Value').reset_index()
	df.columns.name = None
	# 파일저장
	df.to_pickle(oecd_save_folder+'OECD_Production,Sales&Orders.pkl')

	# KEI_Group2----------------------------------------------------------------------------------
	file = folder + 'KEI_Group2.csv'
	cols = ['SUBJECT', 'LOCATION', 'TIME', 'Value', 'Country']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	# 영문을 한글로 변경
	df = country_eng_to_kor(df)  # 국가명 한글화
	df = name_eng_to_kor(df)  # 통계명 한글화
	df.rename(columns={'TIME': '날짜'}, inplace=True)
	df['주기'] = '월간'
	df['출처'] = 'OECD'
	cols = ['날짜', '국가명', 'Country', '소득그룹', '지역', '주기', '출처']
	df = df.pivot_table(index=cols, columns='통계명', values='Value').reset_index()
	df.columns.name = None
	# 파일저장
	df.to_pickle(oecd_save_folder+'OECD_Group2.pkl')

 