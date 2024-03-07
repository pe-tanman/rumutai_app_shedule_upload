from pandas.core.indexes.timedeltas import timedelta_range
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import pandas as pd

##設定##
original_excel_path = "2023early_schedule.xlsx" #日程表のパス
sheet_names = ["一日目", '二日目']#メインの日程表のシート名
time_row = 7#開始時間が書かれている行-1を指定 (第8行→7)
place_column = 1#試合場所が書かれている列-1を指定 (B列→1)
season = 'Kouki'#Kouki or Zenki
credential_file_path = "rumutai-6b4ce-firebase-adminsdk-v4vb7-179084a91e.json"#credential_fileはfirestoreのサービスアカウント設定から取得できる。ぜったいに公開リポジトリに入れない。
######

#データのよみこみ 
def ReadGamesFromExcel(row, col, original_df):
  if not pd.isna(original_df.iloc[row, col]):#nullチェック 
    id = original_df.iloc[row+3, col].lower() #小文字に変換する
    team1 = str(original_df.iloc[row+1, col])
    team2 = str(original_df.iloc[row+2, col])
    time = str(original_df.iloc[time_row, col])
    place = original_df.iloc[row, place_column]
    
    #時刻の分割
    hour_and_min = time.split(":")
    hour = hour_and_min[0]
    minute = hour_and_min[1]

    #DataFrameに変換 
    data = pd.DataFrame(data=[[id, team1, team2, place, hour, minute]], columns= column_names)
    return data

#整形
def SortedDataframe(original_df):
  aligned_df = pd.DataFrame(index = [], columns = column_names)
  
  for i in range(time_row + 1, len(original_df)):#行ごとに読み込む
    if (i - (time_row + 1)) % 4 == 0:#４行ずつ読み込む
      if pd.isna(original_df.iloc[i, place_column]): #下端に辿り着いたらループ解除
        break
      for j in range (place_column + 1, len(original_df.columns)):#列ごとに読み込み
        if pd.isna(original_df.iloc[time_row, j]):#右端に辿り着いたらループ解除
          break
        aligned_df = pd.concat([aligned_df,ReadGamesFromExcel(i, j, original_df)], ignore_index=True)

  sorted_df = aligned_df.sort_values("id")#idじゅんに並び替え
  return sorted_df

def UploadGameData(date, df):
  for r in range(0,len(df)):
    #データの再取得と補充
      extracted_df = df.loc[r]
      id = extracted_df["id"]
      team = {"0":extracted_df["team1"], "1":extracted_df["team2"]}
      place = extracted_df["place"]
      startTime = {"date":date, "hour":extracted_df["hour"], "minute": extracted_df["minute"]}
      if "rumutaiStaff" in df.columns:
        rumutaiStaff = extracted_df["rumutaiStaff"]
      else:
        rumutaiStaff = ""
      referees = {"0":'', "1":'', "2":''}
      extraTime = ""
      gameStatus = "before"
      score=["", ""]
      scoreDetail = {"0":["",""], "1":["", ""], "2":["", ""]}

      #sportの判定
      sports = initial_data['sports']
      category = id.split("-")[0]
      if(category in sports.keys()):
         sport = sports[category]
      else:
         sport = ""
      
      #アップロード
      doc_ref = collection_ref.document(id)
      doc_ref.set({
          'gameId': id,
          'team':team,
          'place': place,
          'startTime': startTime,
          "referees": referees,
          "rumutaiStaff":rumutaiStaff,
          'sport': sport,
          "score":score,
          "scoreDetail":scoreDetail,
          "extraTime":extraTime,
          "gameStatus":gameStatus,
      }, merge = True)


#Excelファイルの読み込み
multi_excel_sheets = pd.read_excel(original_excel_path, sheet_name=None)
df_sheets = []
column_names = ["id", "team1", "team2", "place", "hour", "minute"]

#Firebaseの初期設定
if not firebase_admin._apps:
  cred = credentials.Certificate('rumutai-6b4ce-firebase-adminsdk-v4vb7-179084a91e.json')
  firebase_admin.initialize_app(cred)
db = firestore.client()

if season == 'Zenki':
   collection_ref = db.collection("gameDataZenki")
elif season == 'Kouki':
   collection_ref = db.collection("Test1")#TODO
else:
   print("seasonの値が正しく指定されていません")
   exit()

initial_data_ref = db.collection("dataForInit").document("dataForInitDoc")
initial_data = initial_data_ref.get().to_dict()


#必要なシートを取り出す
for sheet_name in multi_excel_sheets.keys():
    if sheet_name in sheet_names:
      df = multi_excel_sheets[sheet_name] # sheet_nameのシートをDataFrameとしてdfへ
      df_sheets.append(df)

#試合じょうほうを読み取って整形する
sorted_sheets = []
for df in df_sheets:
   sorted_sheets.append(SortedDataframe(df))

for sorted_sheet in sorted_sheets:
  print(sorted_sheet)

confirm = input("表がうまく読み取れているか確認して問題なければ「y」と入力してください")
if confirm == "y":
  for i in range(2):
   UploadGameData(i, sorted_sheets[i])

