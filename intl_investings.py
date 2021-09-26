import pandas as pd
import requests
from bs4 import BeautifulSoup
import conn_db, helper

user_agent = helper.user_agent
path = conn_db.get_path('investings_raw')

def _make_df_from_investings(url):
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
		return df.dropna().reset_index(drop=True)

@helper.timer
def get_data_from_investings():
	investings_dict = {'https://kr.investing.com/indices/phlx-semiconductor-historical-data': '필라델피아 반도체지수',
						'https://kr.investing.com/indices/usdollar-historical-data': 'Dollar지수',
						'https://kr.investing.com/currencies/usd-krw-historical-data': 'USD_KRW',
						'https://kr.investing.com/commodities/crude-oil-historical-data': 'WTI유',
						'https://kr.investing.com/indices/msci-world-historical-data': 'MSCI WORLD',
						'https://kr.investing.com/indices/msci-em-historical-data': 'MSCI EM'}

	cols = ['날짜', '종가']
	df_all = pd.DataFrame()

	for url in investings_dict.keys():
		new = _make_df_from_investings(url)
		name = investings_dict[url]
		filename = path + f'{name}_취합본.pkl'
		old = pd.read_pickle(filename)
		df = pd.concat([new, old]).drop_duplicates(subset='날짜')
		df = df.sort_values(by=['날짜'], ascending=False).reset_index(drop=True)
		df.to_pickle(filename)

		df = df[cols].rename(columns={'종가': name})
		df_all = df_all.append(df)

	for col in df_all.columns.tolist()[1:]:
		if df_all[col].str.contains(',').sum() > 0:
			df_all[col] = df_all[col].str.replace(',', '')
		df_all[col] = df_all[col].astype('float')
	df_all = df_all.groupby('날짜').agg('mean').reset_index()
	df_all = df_all.sort_values(by=['날짜'], ascending=False).reset_index(drop=True)

	df_all.to_pickle(conn_db.get_path('investings') + 'investings_취합본.pkl')