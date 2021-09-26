import pandas as pd 
import requests, time
from bs4 import BeautifulSoup  
import conn_db, helper 
import knoema
apicfg = knoema.ApiConfig() #Knoema 권한 셋팅

apicfg.host = helper.knoema_apicfg.host
apicfg.app_id = helper.knoema_apicfg.app_id
apicfg.app_secret = helper.knoema_apicfg.app_secret

def get_oecd_key_short_term_economic_indicators(): # OECD Key Short-Term Economic Indicators, Monthly Update
	subject_list = ['PRINTO01','PRMNTO01','PRCNTO01','SLRTTO01','SLRTCR03','ODCNPI03','LOLITOAA',
				'LORSGPRT','BSCICP02','CSCICP02','LFEMTTTT','LRHUTTTT','LCEAMN01','ULQEUL01',
				'PIEAMP01','CPALTT01','MANMM101','MABMM301','IRSTCI01','IR3TIB01','CCUSMA02',
				'XTEXVA01','IRLTLT01','XTIMVA01','B6BLTT02','NAEXKP01','NAEXKP02',
				'NAEXKP03','NAEXKP04','NAEXKP06','NAEXKP07']
	df = pd.DataFrame()
	for subject in subject_list:
		try:
			temp_df = knoema.get('OECDKSTEI2018', frequency='M;Q',
							Country='AUS;CAN;CHL;FRA;DEU;ITA;JPN;KOR;MEX;PRT;ESP;TUR;GBR;USA;EA19;EU28;EU27_2020;G-7;OECDE;G-20;OECD;ARG;BRA;CHN;IND;IDN;RUS;SAU;ZAF;GRC',
							Subject=subject, Measure='ST')
			temp_df = pd.pivot_table(temp_df.T, columns = ['Country','Subject','Measure','Frequency']).dropna().reset_index()
			temp_df = temp_df.drop('Measure', axis=1).rename({0:'Value', 'level_0':'Date'}, axis=1)
			temp_df[['Country', 'Subject', 'Frequency']] = temp_df[['Country', 'Subject', 'Frequency']].astype('category')
			df = df.append(temp_df, ignore_index=True)
		except:
			pass
	path = r'C:\Users\bong2\OneDrive\DataArchive\DB_OECD\OECD_Key Short-Term Economic Indicators\OECD_Key Short-Term Economic Indicators.hyper'
	pantab.frame_to_hyper(df, path, table='oecd_short_term_indicator')

def get_pmi(): # Purchasing Managers Index(PMI)
	df = knoema.get(
					'WLDTGEPMI2019',
					timerange = '2002M1-2020M12',
					frequency = 'M',
					Country = '160;181;11;113;13;115;16;188;190;65;299;194;195;71;197;120;121;122;201;125;202;126;75;131;133;32;87;213;167;90;35;PK;143;215;39;144;218;146;147;97;148;223;224;225;151;153;226;103;155;228;48;157;105',
					Indicator = 'KN.I1;KN.I2;KN.I3')
	df = pd.DataFrame(pd.pivot_table(df.T,
									columns = ['Country','Indicator','Frequency'])).dropna()
	df.reset_index(inplace=True)
	df.rename(columns={'level_0':'Date', 0:'Value'}, inplace=True)
	path = r'C:\Users\bong2\OneDrive\DataArchive\DB_Knoema\10_기타\22_Purchasing Managers Index\Purchasing Managers Index(PMI)_{}.csv'.format('202002')
	df.to_csv(path, index=False)
