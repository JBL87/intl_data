from glob import glob
import pandas as pd
import conn_db
import helper

oecd_path = conn_db.get_path('oecd_save')

def _country_eng_to_kor(df):  # 국가명 한글화
	cols = ['3자리', '국가명', '소득그룹', '지역', '국가구분']
	temp = conn_db.from_('Master_국가명', '국가명')[cols]
	df = df.merge(temp, left_on='LOCATION', right_on='3자리', how='left')
	df.drop(columns=['3자리', 'LOCATION'], inplace=True)
	return df

def _name_eng_to_kor(df):  # 통계명 한글화
	temp = conn_db.from_('Master_OECD', '번역용')[['SUBJECT', '통계명']]
	temp = temp.drop_duplicates().reset_index(drop=True)
	df = df.merge(temp, on='SUBJECT', how='left')
	df.drop(columns=['SUBJECT'], inplace=True)
	return df

# OECD 경기선행지수
def composite_leading_indicator()
	file = glob(conn_db.get_path('oecd_Composite Leading Indicators_raw')+'*.csv')
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
	df = _country_eng_to_kor(df)  # 국가명 한글화
	df = _name_eng_to_kor(df)  # 통계명 한글화
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
	df.to_pickle(oecd_path+'OECD_Composite Leading Indicators.pkl')

# OECD KEI 
def key_short_term_economic_indicators() 
	# Master 시트에 업로드
	file = conn_db.get_path('OECD_Key Short-Term Economic Indicators_raw') + 'KEI_master.csv'
	cols = ['SUBJECT', 'Subject', 'Measure', 'Unit']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	df.sort_values(by=cols, inplace=True)
	df = df.drop_duplicates().reset_index(drop=True)
	conn_db.to_(df, 'Master_OECD', 'Key Short-Term Economic Indicators')
	
	# KEI_Production,Sales&Orders
	file = folder + 'KEI_Production,Sales&Orders.csv'
	cols = ['SUBJECT', 'LOCATION', 'TIME', 'Value', 'Country']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	
	# 영문을 한글로 변경
	df = _country_eng_to_kor(df)  # 국가명 한글화
	df = _name_eng_to_kor(df)  # 통계명 한글화
	df.rename(columns={'TIME': '날짜'}, inplace=True)
	df['주기'] = '월간'
	df['출처'] = 'OECD'
	cols = ['날짜', '국가명', 'Country', '소득그룹', '지역', '주기', '출처']
	df = df.pivot_table(index=cols, columns='통계명', values='Value').reset_index()
	df.columns.name = None
	
	# 파일저장
	df.to_pickle(oecd_path+'OECD_Production,Sales&Orders.pkl')

	# KEI_Group2----------------------------------------------------------------------------------
	file = folder + 'KEI_Group2.csv'
	cols = ['SUBJECT', 'LOCATION', 'TIME', 'Value', 'Country']
	df = pd.read_csv(file, encoding='utf-8', usecols=cols)
	
	# 영문을 한글로 변경
	df = _country_eng_to_kor(df)  # 국가명 한글화
	df = _name_eng_to_kor(df)  # 통계명 한글화
	df.rename(columns={'TIME': '날짜'}, inplace=True)
	df['주기'] = '월간'
	df['출처'] = 'OECD'
	cols = ['날짜', '국가명', 'Country', '소득그룹', '지역', '주기', '출처']
	df = df.pivot_table(index=cols, columns='통계명', values='Value').reset_index()
	df.columns.name = None
	
	# 파일저장
	df.to_pickle(oecd_path+'OECD_Group2.pkl')

 
