from pandas.core.indexes.timedeltas import timedelta_range
import sys
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import pandas as pd

##設定
original_excel_path = "2023early_referees.xlsx" #日程表のパス
sheet_name = "一覧表"#メインの日程表のシート名
head_row = 1 #先頭の行-1を指定　(行2→1)
id_column = 3#試合コードが書かれている列-1を指定 (D列→3)
chief_referee_number_column = 44#試合場所が書かれている列-1を指定 (AS列→44)
season = 'Kouki'#Kouki or Zenki
credential_file_path = "rumutai-6b4ce-firebase-adminsdk-v4vb7-179084a91e.json"#credential_fileはfirestoreのサービスアカウント設定から取得できる。ぜったいに公開リポジトリに入れない。
##


#データのよみこみ 
def ReadRefereesFromExcel(row, original_df):
  id = original_df.iloc[row, id_column].lower()
  referee0 = original_df.iloc[row, chief_referee_number_column]
  referee1 = original_df.iloc[row, chief_referee_number_column+4]
  referee2 = original_df.iloc[row, chief_referee_number_column+8]

  #DataFrameに変換 
  data = pd.DataFrame(data=[[id, referee0, referee1, referee2]], columns= column_names)
  return data

#整形
def SortedDataframe(original_df):
  aligned_df = pd.DataFrame(index = [], columns = column_names)
  for i in range(head_row + 1, len(original_df)):#行ごとに読み込む
    if pd.isna(original_df.iloc[i, id_column]): #下端に辿り着いたらループ解除
      break
    aligned_df = pd.concat([aligned_df,ReadRefereesFromExcel(i, original_df)], ignore_index=True)

  sorted_df = aligned_df.sort_values("id")#id順に並び替え
  return sorted_df


def UploadRefereeData(df):
  for r in range(0,len(df)):
    #データの再取得と補充
      extracted_df = df.loc[r]
      id = extracted_df["id"]
      referees = {'0': extracted_df["referee0"], '1': extracted_df["referee1"], '2': extracted_df["referee2"] }
      
      #アップロード
      doc_ref = collection_ref.document(id)
      doc_ref.set({
          'referees': referees
      }, merge = True)

if not firebase_admin._apps:
  cred = credentials.Certificate('rumutai-6b4ce-firebase-adminsdk-v4vb7-179084a91e.json')
  firebase_admin.initialize_app(cred)
  
db = firestore.client()

if season == 'Zenki':
   collection_ref = db.collection("gameDataZenki")
elif season == 'Kouki':
   collection_ref = db.collection("Test1")
else:
   print("seasonの値が正しく指定されていません")
   exit()

#Excelファイルの読み込み
column_names = ["id", "referee0", 'referee1', 'referee2']
excel_sheet_df = pd.read_excel(original_excel_path, sheet_name=sheet_name)
sorted_sheet = SortedDataframe(excel_sheet_df)
print(sorted_sheet)

confirm = input("表がうまく読み取れているか確認して問題なければ「y」と入力してください")
if confirm == "y":
  print("uploading...")
  UploadRefereeData(sorted_sheet)