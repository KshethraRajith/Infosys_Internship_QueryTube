import pandas as pd
import re

# Load files
file1 = pd.read_csv("cleaned_youtube_data.csv")
file2 = pd.read_csv("intellipaat_last_50.csv")

# ----------------------
# FIX COLUMN NAME IN SECOND FILE
# ----------------------
file2.rename(columns=lambda x: x.strip().lower(), inplace=True)
file1.rename(columns=lambda x: x.strip().lower(), inplace=True)

# Rename "video id" → "id"
if "video id" in file2.columns:
    file2.rename(columns={"video id": "id"}, inplace=True)

# ----------------------
# 1. MERGE DATASETS
# ----------------------
merged = pd.merge(file1, file2, on="id", how="left")
print("After merge shape:", merged.shape)

# ----------------------
# REMOVE EMPTY _y COLUMNS
# ----------------------
cols_to_drop = [col for col in merged.columns if col.endswith("_y")]
merged.drop(columns=cols_to_drop, inplace=True)
print("Removed _y columns:", cols_to_drop)

# ----------------------
# REMOVE _x SUFFIXES
# ----------------------
merged.rename(columns=lambda c: c.replace("_x", ""), inplace=True)

# ----------------------
# 2. CLEAN TITLE + TRANSCRIPT
# ----------------------
def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return text

if "title" in merged.columns:
    merged["title"] = merged["title"].apply(clean_text)

if "transcript" in merged.columns:
    merged["transcript"] = merged["transcript"].apply(clean_text)

# ----------------------
# 3. REMOVE DUPLICATES (based on id)
# ----------------------
if "id" in merged.columns:
    merged = merged.drop_duplicates(subset=["id"])
else:
    merged = merged.drop_duplicates()

print("After removing duplicates:", merged.shape)

# ----------------------
# 4. KEEP ONLY FIRST 50 ROWS
# ----------------------
merged = merged.head(50)

# ----------------------
# 5. CONVERT DURATION TO SECONDS
# ----------------------
def convert_duration(d):
    if pd.isna(d):
        return 0
    hours = minutes = seconds = 0

    h = re.search(r"(\d+)H", d)
    m = re.search(r"(\d+)M", d)
    s = re.search(r"(\d+)S", d)

    if h: hours = int(h.group(1))
    if m: minutes = int(m.group(1))
    if s: seconds = int(s.group(1))

    return hours * 3600 + minutes * 60 + seconds

if "duration" in merged.columns:
    merged["duration_seconds"] = merged["duration"].apply(convert_duration)

# ----------------------
# SAVE FINAL FILE
# ----------------------
merged.to_csv("final_cleaned_dataset.csv", index=False)
print("✅ Final dataset saved as: final_dataset.csv")