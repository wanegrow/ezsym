from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
import logging
import sys
import urllib.parse
import requests
import re
import time
import threading
import uuid
import shutil
from pathlib import Path
from collections import deque

logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(module)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

app = Flask(__name__)
CORS(app)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# --- DEDUPLICATION CONFIG ---
processed_hashes = {}  # Store {hash: timestamp}
HASH_LOCK_TIME = 30    # Seconds to ignore duplicate requests
hash_lock = threading.Lock()

class TaskWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
    def run(self):
        while self.running:
            try:
                task_id, raw_data, torrent_id = None, None, None
                with queue_lock:
                    if request_queue:
                        task_id, raw_data, torrent_id = request_queue.popleft()
                        active_tasks[task_id] = torrent_id
                if raw_data:
                    with task_semaphore:
                        try:
                            start_time = time.time()
                            with app.test_request_context(method="POST", data=raw_data, headers={"Content-Type": "application/json"}):
                                data = request.get_json()
                                response = process_symlink_creation(data, task_id)
                                if response[1] != 200:
                                    logging.error(f"Task {task_id} failed: {response[0].get_json()}")
                        except Exception as e:
                            logging.error(f"Task {task_id} processing failed: {str(e)}")
                            with download_lock:
                                if task_id in download_statuses:
                                    download_statuses[task_id]['status'] = 'failed'
                                    download_statuses[task_id]['error'] = str(e)
                        finally:
                            with queue_lock:
                                if task_id in active_tasks:
                                    del active_tasks[task_id]
                            logging.info(f"Task {task_id} completed in {time.time()-start_time:.1f}s")
                else:
                    time.sleep(1)
            except Exception as e:
                logging.error(f"Queue worker error: {str(e)}")
                time.sleep(5)

RD_API_KEY = os.getenv("RD_API_KEY")
MEDIA_SERVER = os.getenv("MEDIA_SERVER", "plex").lower()
ENABLE_DOWNLOADS = os.getenv("ENABLE_DOWNLOADS", "false").lower() == "true"
MOVE_TO_FINAL_LIBRARY = os.getenv("MOVE_TO_FINAL_LIBRARY", "true").lower() == "true"
SYMLINK_BASE_PATH = Path(os.getenv("SYMLINK_BASE_PATH", "/symlinks"))
DOWNLOAD_COMPLETE_PATH = Path(os.getenv("DOWNLOAD_COMPLETE_PATH", "/dl_complete"))
FINAL_LIBRARY_PATH = Path(os.getenv("FINAL_LIBRARY_PATH", "/library"))
RCLONE_MOUNT_PATH = Path(os.getenv("RCLONE_MOUNT_PATH", "/mnt/zurg/__all__"))
PLEX_TOKEN = os.getenv("PLEX_TOKEN")
PLEX_LIBRARY_NAME = os.getenv("PLEX_LIBRARY_NAME")
PLEX_SERVER_IP = os.getenv("PLEX_SERVER_IP")
EMBY_SERVER_IP = os.getenv("EMBY_SERVER_IP")
EMBY_API_KEY = os.getenv("EMBY_API_KEY")
EMBY_LIBRARY_NAME = os.getenv("EMBY_LIBRARY_NAME")
MAX_CONCURRENT_TASKS = int(os.getenv("MAX_CONCURRENT_TASKS", "3"))
DELETE_AFTER_COPY = os.getenv("DELETE_AFTER_COPY", "false").lower() == "true"
REMOVE_WORDS = [w.strip() for w in os.getenv("REMOVE_WORDS", "").split(",") if w.strip()]
SCAN_DELAY = int(os.getenv("SCAN_DELAY", "60"))

plex_section_id = None
plex_initialized = False
task_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_TASKS)
queue_lock = threading.Lock()
request_queue = deque()
active_tasks = {}
download_statuses = {}
download_lock = threading.Lock()

@app.route('/rd-proxy', methods=['POST'])
def rd_proxy():
    try:
        data = request.get_json()
        endpoint = data.get('endpoint', '')
        method = data.get('method', 'GET').upper()
        payload = data.get('data', None)

        if not RD_API_KEY:
            return jsonify({"error": "Server configuration error"}), 500

        # --- DEDUPLICATION FOR PROXY ---
        if "addMagnet" in endpoint:
            magnet_hash = re.search(r'xt=urn:btih:([a-z0-9]+)', str(payload), re.I)
            if magnet_hash:
                h = magnet_hash.group(1).upper()
                with hash_lock:
                    now = time.time()
                    if h in processed_hashes and (now - processed_hashes[h]) < HASH_LOCK_TIME:
                        return jsonify({"status": "success", "message": "Duplicate magnet suppressed"}), 200
                    processed_hashes[h] = now

        response = requests.request(
            method,
            f"https://api.real-debrid.com/rest/1.0{endpoint}",
            headers={"Authorization": f"Bearer {RD_API_KEY}"},
            data=payload,
            timeout=15
        )
        
        if response.status_code in [200, 202, 204] and not response.text.strip():
            return jsonify({"status": "success"}), 200

        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# [get_plex_section_id, trigger_plex_scan, trigger_media_scan functions here...]

def clean_filename(original_name):
    cleaned = original_name
    for pattern in REMOVE_WORDS:
        cleaned = re.sub(rf"{re.escape(pattern)}", "", cleaned, flags=re.IGNORECASE)
    name_part, ext_part = os.path.splitext(cleaned)
    name_part = re.sub(r"[\W_]+", "-", name_part).strip("-")
    return f"{name_part or 'file'}"

def process_symlink_creation(data, task_id):
    with download_lock:
        if task_id not in download_statuses:
            download_statuses[task_id] = {"status": "queued", "progress": 0, "speed": 0, "error": None}

    try:
        torrent_id = data['torrent_id']
        torrent_info = requests.get(f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}",
                                  headers={"Authorization": f"Bearer {RD_API_KEY}"},
                                  timeout=15).json()

        selected_files = [f for f in torrent_info["files"] if f.get("selected") == 1]
        base_name = clean_filename(os.path.splitext(torrent_info["filename"])[0])
        dest_dir = SYMLINK_BASE_PATH / base_name
        dest_dir.mkdir(parents=True, exist_ok=True)

        # DEFINE STRUCTURE: RCLONE_MOUNT_PATH is /mnt/zurg/__all__
        zurg_root = RCLONE_MOUNT_PATH.parent # This becomes /mnt/zurg
        
        # Possible search locations for the file
        search_dirs = [
            zurg_root / "scenes" / base_name,
            RCLONE_MOUNT_PATH / torrent_info["filename"],
            zurg_root / "__downloads__" / base_name,
        ]

        created_paths = []
        for file in selected_files:
            file_path_raw = Path(file["path"].lstrip("/"))
            dest_path = dest_dir / f"{clean_filename(file_path_raw.stem)}{file_path_raw.suffix.lower()}"
            
            # Junk Filter
            if "996gg" in str(file_path_raw).lower():
                continue

            src_path = None
            # Find the actual file in Zurg's sibling folders
            for s_dir in search_dirs:
                potential = s_dir / file_path_raw.name
                if potential.exists():
                    src_path = potential
                    break
            
            # Final fallback to direct path
            if not src_path:
                src_path = RCLONE_MOUNT_PATH / torrent_info["filename"] / file_path_raw

            if src_path and src_path.exists():
                if os.path.lexists(dest_path):
                    dest_path.unlink()
                dest_path.symlink_to(src_path)
                logging.info(f"SUCCESS: {dest_path.name} -> {src_path}")
                created_paths.append(str(dest_path))

        return jsonify({"status": "processed", "created_paths": created_paths, "task_id": task_id}), 200

    except Exception as e:
        logging.error(f"CRITICAL ERROR: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/symlink", methods=["POST"])
def create_symlink():
    data = request.get_json()
    torrent_id = data.get("torrent_id")
    item_hash = data.get("hash", torrent_id)

    with hash_lock:
        now = time.time()
        if item_hash in processed_hashes and (now - processed_hashes[item_hash]) < HASH_LOCK_TIME:
            return jsonify({"status": "success", "message": "Duplicate suppressed"}), 200
        processed_hashes[item_hash] = now

    task_id = str(uuid.uuid4())
    return process_symlink_creation(data, task_id)

if __name__ == "__main__":
    workers = [TaskWorker() for _ in range(MAX_CONCURRENT_TASKS * 2)]
    for w in workers:
        w.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5002")), threaded=True)
