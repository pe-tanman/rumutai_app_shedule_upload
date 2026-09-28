# 📋 HR対抗 Schedule Uploader

> *From the organizers' Excel sheet to every student's phone in one command.*

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-150458?logo=pandas&logoColor=white)
![Firestore](https://img.shields.io/badge/Firestore-FFCA28?logo=firebase&logoColor=black)


## 🌟 Highlights

- 📥 **Reads the official Excel files** — the organizers' schedule and referee tables are used as-is, with no retyping
- 🔄 **Uploads straight to Firestore** — every game shows up in the [HR対抗 app](https://github.com/pe-tanman/rumutai_app) with its teams, venue and start time
- ✅ **Preview before upload** — prints the parsed tables and waits for your `y` before writing anything
- 🧑‍⚖️ **Referee assignments** — a second script fills in the chief and assistant referees for every game
- 📦 **Standalone executables** — PyInstaller builds in `dist/` let committee members without Python run the tools


## ℹ️ Overview

This is the admin-side companion to the [**HR対抗 app**](https://github.com/pe-tanman/rumutai_app), which runs Asahigaoka High School's inter-homeroom sports tournament. The tournament committee plans games in a big Excel grid. These scripts parse that grid and create one Firestore document per game. The app then displays each game and updates it live as results come in.

| Script | Input | Writes |
| --- | --- | --- |
| `schedule_table_upload.py` | `schedule.xlsx` (sheets `一日目`, `二日目`) | Game ID, teams, venue, date and time, sport, empty score fields |
| `referees_table_upload.py` | `referees.xlsx` (sheet `一覧表`) | Three referees per game |


### ✍️ Author

Written by [Yuki Ishihara](https://github.com/pe-tanman) for the Asahigaoka High School Student Council app committee.


## 🚀 Usage

```bash
git clone https://github.com/pe-tanman/rumutai_app_shedule_upload.git
cd rumutai_app_shedule_upload
pip install pandas openpyxl firebase-admin
```

1. Download a Firestore **service-account key** (Firebase Console → Project settings → Service accounts) into this folder.
2. Open the script and check the **設定 (settings)** block at the top: the Excel path, sheet names, row and column positions, `season` (`'Zenki'` for the first term, `'Kouki'` for the second) and the credential filename.
3. Put `schedule.xlsx` / `referees.xlsx` next to the script and run:

   ```bash
   python schedule_table_upload.py
   python referees_table_upload.py
   ```

4. Check the printed tables and type **`y`** to upload.

> [!WARNING]
> Never commit the service-account JSON or the Excel files, since they contain student information. Add them to `.gitignore`.

> [!NOTE]
> In `referees_table_upload.py`, `season = 'Kouki'` currently writes to a `Test1` collection instead of `gameDataKouki`. Update that line before a real upload.


## 💭 Feedback

Future committees: feel free to fork this and adapt the row and column settings to your own spreadsheet layout. Questions are welcome in [Issues](https://github.com/pe-tanman/rumutai_app_shedule_upload/issues).
