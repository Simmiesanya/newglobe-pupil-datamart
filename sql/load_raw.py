import pandas as pd
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

DB_PATH = PROJECT_ROOT / "newglobe_pupil_datamart.db"

PUPIL_CSV = PROJECT_ROOT / "data" / "PupilData.csv"
ATTENDANCE_CSV = PROJECT_ROOT / "data" / "PupilAttendance.csv"

pupil_df = pd.read_csv(PUPIL_CSV)
attendance_df = pd.read_csv(ATTENDANCE_CSV)

conn = sqlite3.connect(DB_PATH)

pupil_df.to_sql('raw_pupil', conn, if_exists='replace', index=False)
attendance_df.to_sql('raw_pupil_attendance', conn, if_exists='replace', index=False)

conn.close()

print(f"Loaded {len(pupil_df)} rows into raw_pupil")
print(f"Loaded {len(attendance_df)} rows into raw_pupil_attendance")