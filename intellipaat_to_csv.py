import os
import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv
import isodate

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing YOUTUBE_API_KEY in .env")

youtube = build("youtube", "v3", developerKey=API_KEY)

CHANNEL_NAME = "Intellipaat"
MAX_VIDEOS = 50
OUT_FILE = "intellipaat_last_50.csv"

# ---- Find channel by name ----
search = youtube.search().list(
    q=CHANNEL_NAME, type="channel", part="snippet", maxResults=1
).execute()

if not search.get("items"):
    raise RuntimeError("Channel not found")

channel_id = search["items"][0]["snippet"]["channelId"]

# ---- Get channel details ----
ch = youtube.channels().list(
    id=channel_id, part="snippet,statistics,brandingSettings,contentDetails"
).execute()["items"][0]

channel_title = ch["snippet"].get("title")
channel_description = ch["snippet"].get("description")
channel_country = ch["snippet"].get("country")
channel_thumbnail = ch["snippet"]["thumbnails"]["high"]["url"]
channel_subscriberCount = ch["statistics"].get("subscriberCount")
channel_videoCount = ch["statistics"].get("videoCount")

# ---- Get uploads playlist ----
uploads_id = ch["contentDetails"]["relatedPlaylists"]["uploads"]

video_ids = []
next_page = None
while len(video_ids) < MAX_VIDEOS:
    pl = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=uploads_id,
        maxResults=50,
        pageToken=next_page
    ).execute()
    for it in pl["items"]:
        video_ids.append(it["contentDetails"]["videoId"])
        if len(video_ids) == MAX_VIDEOS:
            break
    next_page = pl.get("nextPageToken")
    if not next_page:
        break

# ---- Fetch video details in batches of 50 ----
rows = []

def chunks(lst, n=50):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

for batch in chunks(video_ids):
    vids = youtube.videos().list(
        part="snippet,contentDetails,statistics,status",
        id=",".join(batch)
    ).execute()

    for v in vids["items"]:
        sn = v["snippet"]
        cd = v["contentDetails"]
        st = v.get("statistics", {})
        ss = v.get("status", {})

        thumbs = sn.get("thumbnails", {})
        thumb_default = thumbs.get("default", {}).get("url")
        thumb_high = thumbs.get("high", {}).get("url")

        # duration in ISO 8601 to HH:MM:SS
        try:
            dur = str(isodate.parse_duration(cd.get("duration")))
        except:
            dur = None

        rows.append({
            "video id": v.get("id"),
            "title": sn.get("title"),
            "description": sn.get("description"),
            "publishedAt": sn.get("publishedAt"),
            "tags": ",".join(sn.get("tags", [])) if sn.get("tags") else None,
            "categoryId": sn.get("categoryId"),
            "defaultLanguage": sn.get("defaultLanguage"),
            "defaultAudioLanguage": sn.get("defaultAudioLanguage"),
            "thumbnail_default": thumb_default,
            "thumbnail_high": thumb_high,
            "duration": dur,
            "viewCount": st.get("viewCount"),
            "likeCount": st.get("likeCount"),
            "commentCount": st.get("commentCount"),
            "privacyStatus": ss.get("privacyStatus"),
            "channel_id": channel_id,
            "channel_title": channel_title,
            "channel_description": channel_description,
            "channel_country": channel_country,
            "channel_thumbnail": channel_thumbnail,
            "channel_subscriberCount": channel_subscriberCount,
            "channel_videoCount": channel_videoCount,
        })

# ---- Save CSV ----
df = pd.DataFrame(rows)
df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")
print(f"Saved: {OUT_FILE} ({len(df)} videos)")
