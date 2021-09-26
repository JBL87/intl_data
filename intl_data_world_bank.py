from glob import glob
import pandas as pd
import conn_db
import helper

# World Bank_Population-Estimates에 있는 국가명
def wolrld_bank_country_name():
	cols = ['Country Code', 'Short Name', 'Table Name',
	        'Long Name', '2-alpha code', 'Region',
	        'Income Group', 'WB-2 code', ]
	df = pd.read_csv(conn_db.get_path('wolrld_bank_population_raw')+".csv",
                  encoding='utf-8', usecols=cols)
	conn_db.to_(df, 'Master_국가명', '국가명_import_from_WorldBank')

	# World Bank_World Development Indicators
	# 지표 meta data 구글 업로드
	# 작업중
	# cols = ['Series Code', 'Topic', 'Indicator Name', 'Unit of measure',
	#         'Periodicity', 'Base Period', 'Aggregation method', 'Source']
	# df = pd.read_csv(conn_db.get_path('wolrld_bank_wdi_raw')+".csv",
    #               encoding='utf-8', usecols=cols)
	# df[['주제', '통계명']] = df['Topic'].str.split(':', 1, expand=True)
	# df.drop(columns=['Topic'], inplace=True)

# world bank comodity 월별 데이터 전처리
def clean_world_bank_comodity():
	file = conn_db.get_path('world_bank_comodity_raw')+'CMOHistoricalDataMonthly.xlsx'
	df = pd.read_excel(file, skiprows=4, encoding='utf-8',
	                   sheet_name='Monthly Prices')
	
	unit = df.iloc[0, ].tolist()[1:]
	code = df.iloc[1, ].tolist()[1:]
	cols = df.columns.tolist()[1:]
	new_cols = []
	for x, y, z in zip(cols, unit, code):
		new_cols.append(x + ' ' + y + '-' + z)
	
	new_cols = ['날짜'] + new_cols
	df.columns = new_cols
	df = df.iloc[2:, ].copy()
	drop_cols = ['COAL_COL', 'COPRA', 'iSTL_JP_INDX', 'STL_JP_CROLL', 
				'STL_JP_HROLL', 'STL_JP_REBAR','STL_JP_WIROD', 'COTTON_MEMPH', 
				'RUBBER1_US', 'WHEAT_CANADI', 'WOODPULP']
	
	for drop_col in drop_cols:
		for col in df.columns.tolist():
			if drop_col in col:
				df.drop(columns=col, inplace=True)
	
	
	df['날짜'] = df['날짜'].str.replace('M', '-')
	df = df.melt(id_vars='날짜', var_name='item', value_name='값')
	df['item'] = df['item'].str.split('-', expand=True)[0]
	df['item'] = df['item'].str.replace('  ', ' ').str.strip()
	
	filt = df['값'] == '..'
	df = df.loc[~filt].copy().reset_index(drop=True)
	
	df['값'] = df['값'].astype('float')
	df = df.pivot_table(index='날짜', columns='item', values='값').reset_index()
	df.columns.name = None
	
	path = conn_db.get_path('World Bank_Comodity')+'worldbank_comodity_취합본.pkl'
	df.to_pickle(path)

# World Bank_Global Economic Monitor
def clean_world_bank_global_economic_monitor():
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

