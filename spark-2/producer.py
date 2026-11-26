"""
Music Streaming Producer -simulates a music streaming service producing messages to Kafka topics
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime   
import uuid

print("Music Streaming Kafka Producer is starting...    ")
print("="*50    )

# Define Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Ensure all replicas acknowledge the message
    retries=3  # Number of retries for sending messages
)
print("Kafka Producer initialized successfully.")

# Define music streaming events
artists =  [
    {"name": "the midnight keys", "genre": "synthwave"},
    {"name": "the weeknd", "genre": "pop"},
    {"name": "dua lipa", "genre": "pop"},
    {"name": "taylor swift", "genre": "folk"}  ,
    {"name": "the 1975", "genre": "indie pop"},
    {"name": "arctic monkeys", "genre": "indie rock"},
    {"name": "billie eilish", "genre": "electropop"},
    {"name": "florence + the machine", "genre": "alternative rock"},
    {"name": "the killers", "genre": "the killers"},
    {"name": "the strokes", "genre": "garage rock"},
    {"name": "the black keys", "genre": "blues rock"},
    {"name": "the white stripes", "genre": "ambient rock"}
]

#tracks for each artist
tracks = []
for artist in artists:
    for i in range(1, 4):  # 3 tracks per artist
        track = {
            "track_title": f"{artist['name'].split()[-1]} {random.choice(['Awesome', 'Incredible', 'Fantastic','dreams','waves'])}",
            "artist_name": artist['name'],
            "album_name": f"{random.choice(['Midnight','Eternal','Urban','Digital','Natural'])} {random.choice(['Vibes','Dreams','Waves','Beats','Rhythms'])}",
            "genre": artist['genre'],
            "duration": random.randint(180000, 360000),  # Duration in milliseconds (3 to 6 minutes)
        }
        tracks.append(track)

users = ["melody_lover", "beat_master", "rhythm_seeker", "sound_explorer",
        "tune_fanatic", "harmony_hunter", "beat_enthusiast", "melody_maven", "sound_sculptor", "tune_tamer",
        "harmony_hacker", "beat_believer", "melody_maniac", "sound_savant", "tune_tactician","dj_mixer",
        "beat_boss", "casual_listener", "music_enthusiast", "audio_adventurer"]

actions = [
    {"type": "play", "description": "started playing a track"},
    {"type": "pause", "description": "paused a track"},
    {"type": "skip", "description": "skipped a track"},  
    {"type": "like", "description": "liked a track"},
    {"type": "dislike", "description": "disliked a track"},
    {"type": "share", "description": "shared a track"},
    {"type": "add_to_playlist", "description": "added a track to a playlist"},
    {"type": "remove_from_playlist", "description": "removed a track from a playlist"},
    {"type": "search", "description": "searched for a track or artist"},
    {"type": "view_artist_profile", "description": "viewed an artist's profile"}
]


print("\n Simulating music straming events and sending to Kafka topic 'music-streaming-events'...\n")
print( "Artists with their tracks and genres")
print("="*50)

event_count = 0
session_id = str(uuid.uuid4())[:8]  # Generate a unique session ID for the producer

try:
    while True:
        user = random.choice(users)
        track = random.choice(tracks)
        action = random.choice(actions)

        #calculate play duration based on action type
        if action["type"] == "play":
            #listen to 30-100% of the track duration
            play_duration = int(track["duration"] * random.uniform(0.3, 1.0))
        elif action["type"] == "skip":
            #short listen before skipping, 10-30% of the track duration
            play_duration = random.randint(1000,45000)  # 1 second to 45 seconds
        else:
            play_duration = 0  # No duration for other actions

        #build the event payload
        event = {
            "event_id": str(uuid.uuid4()),  # Unique event ID
            "session_id": session_id,  # Session ID for the producer
            "user_id": user,
            "track_title": track["track_title"],
            "artist_name": track["artist_name"],
            "album_name": track["album_name"],
            "genre": track["genre"],
            "action_type": action["type"],
            "action_description": action["description"],
            "play_duration": play_duration,  # Duration in milliseconds
            "timestamp": int(time.time() * 1000),  # Current timestamp in milliseconds
            "device": random.choice(["mobile", "desktop", "tablet","smart_speaker","car","tv"]),  # Random device type
            "location": random.choice(["New York", "USA", "London", "UK", "Berlin", "Germany", "Tokyo","San Francisco, USA"]) , # Random location 
            "user_tier": random.choice(["free", "premium", "family", "student"]),  # Random user tier
        }

        #send the event to Kafka topic
        producer.send("music-streaming-events", value=event)
        event_count += 1

        #display nicely

        timestamp = datetime.now().strftime("%H:%M:%S")
        duration_sec = play_duration // 1000 if play_duration > 0 else 0  # Convert milliseconds to seconds

        print(f"[{timestamp}] #{event_count:04d} {user:15} |"
            f"{track['artist_name'][:20]:20} | "
            f"{track['track_title'][:20]:20} | "
            f"{action['type']:15} | "
            f"{duration_sec:3d}s | "
            f"{track['genre']:15}"
        )

        #variable sleep time to simulate real user behavior
        time.sleep(random.uniform(0.1, 1.0))      # Sleep for 0.1 to 1 second before sending the next event 


        #change session ID every 100 events to simulate different user sessions
        if random.random() <0.05: # 5% chance to change session ID
            session_id = str(uuid.uuid4())[:8]  # Generate a new unique session ID
            print(f"\nSession ID changed to: {session_id}\n")
except KeyboardInterrupt:
    print("\nStopping producer...")
    print(f"Total events sent: {event_count}")
finally:
    producer.flush()
    producer.close()
    print("Kafka Producer closed successfully.")
