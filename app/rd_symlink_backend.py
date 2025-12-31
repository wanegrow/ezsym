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
RCLONE_MOUNT_PATH = Path(os.getenv("RCLONE_MOUNT_PATH", "/mnt/data/media/remote/realdebrid/__all__"))
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
            app.logger.error("RD_API_KEY missing in environment")
            return jsonify({"error": "Server configuration error"}), 500

        if not endpoint.startswith('/'):
            return jsonify({"error": f"Invalid endpoint format: {endpoint}"}), 400

        response = requests.request(
            method,
            f"https://api.real-debrid.com/rest/1.0{endpoint}",
            headers={
                "Authorization": f"Bearer {RD_API_KEY}",
                "Cache-Control": "no-store, max-age=0"
            },
            data=payload,
            timeout=15
        )
        
        try:
            response.raise_for_status()

            if response.status_code in [200, 202, 204] and not response.text.strip():
                app.logger.info(f"Handled empty success response ({response.status_code})")
                resp = jsonify({"status": "success"})
                resp.headers['Cache-Control'] = 'no-store, max-age=0'
                return resp, 200

            try:
                resp = jsonify(response.json())
                resp.headers['Cache-Control'] = 'no-store, max-age=0'
                return resp, response.status_code
            except json.JSONDecodeError:
                if response.status_code in [200, 202, 204]:
                    app.logger.info(f"Empty success response ({response.status_code})")
                    resp = jsonify({"status": "success"})
                    resp.headers['Cache-Control'] = 'no-store, max-age=0'
                    return resp, 200
                app.logger.error(f"Invalid JSON response | Status: {response.status_code} | Content: {response.text[:200]}")
                resp = jsonify({
                    "source": "Real-Debrid API",
                    "status": response.status_code,
                    "message": response.text
                })
                resp.headers['Cache-Control'] = 'no-store, max-age=0'
                return resp, response.status_code
        except requests.HTTPError as e:
            error_data = {
                "source": "Real-Debrid API",
                "status": e.response.status_code,
                "message": e.response.text
            }
            resp = jsonify(error_data)
            resp.headers['Cache-Control'] = 'no-store, max-age=0'
            return resp, e.response.status_code

    except Exception as e:
        error_details = {
            "exception_type": type(e).__name__,
            "message": str(e),
            "request_data": data,
            "response_content": getattr(e, 'response', {}).text if hasattr(e, 'response') else None
        }
        if hasattr(e, 'response') and e.response.status_code in [200, 202, 204]:
            app.logger.info(f"Handled proxy error for success code: {json.dumps(error_details, indent=2)}")
            resp = jsonify({"status": "success"})
            resp.headers['Cache-Control'] = 'no-store, max-age=0'
            return resp, 200
        app.logger.error(f"Proxy Error Details:\n{json.dumps(error_details, indent=2)}")
        resp = jsonify({
            "error": "Proxy processing failed",
            "details": str(e)
        })
        resp.headers['Cache-Control'] = 'no-store, max-age=0'
        return resp, 500

def get_restricted_links(torrent_id):
    response = requests.get(f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}",
                          headers={"Authorization": f"Bearer {RD_API_KEY}"},
                          timeout=15)
    response.raise_for_status()
    return response.json().get("links", [])

def unrestrict_link(restricted_link):
    response = requests.post("https://api.real-debrid.com/rest/1.0/unrestrict/link",
                           headers={"Authorization": f"Bearer {RD_API_KEY}"},
                           data={"link": restricted_link},
                           timeout=15)
    response.raise_for_status()
    return response.json()["download"]

def clean_filename(original_name):
    cleaned = original_name
    for pattern in REMOVE_WORDS:
        cleaned = re.sub(rf"{re.escape(pattern)}", "", cleaned, flags=re.IGNORECASE)
    name_part, ext_part = os.path.splitext(cleaned)
    name_part = re.sub(r"_(\d+)(?=\.\w+$|$)", r"-cd\1", name_part)
    name_part = re.sub(r"[\W_]+", "-", name_part).strip("-")
    return f"{name_part or 'file'}"

def log_download_speed(task_id, torrent_id, dest_path):
    temp_path = None
    try:
        with download_lock:
            download_statuses[task_id] = {
                "status": "starting",
                "progress": 0.0,
                "speed": 0.0,
                "error": None,
                "dest_path": str(dest_path),
                "filename": Path(dest_path).name
            }

        dest_dir = dest_path.parent
        dest_dir.mkdir(parents=True, exist_ok=True)

        if os.getenv('DOWNLOAD_UID') and os.getenv('DOWNLOAD_GID'):
            os.chown(dest_dir, int(os.getenv('DOWNLOAD_UID')), int(os.getenv('DOWNLOAD_GID')))
            os.chmod(dest_dir, 0o775)

        test_file = dest_dir / "permission_test.tmp"
        try:
            with open(test_file, "w") as f:
                f.write("permission_test")
        except Exception as e:
            logging.warning(f"Permission test failed: {str(e)}")
        finally:
            if test_file.exists():
                test_file.unlink()

        torrent_info = requests.get(f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}",
                                  headers={"Authorization": f"Bearer {RD_API_KEY}"},
                                  timeout=15).json()
        base_name = clean_filename(os.path.splitext(torrent_info["filename"])[0])
        dest_dir = DOWNLOAD_COMPLETE_PATH / base_name
        dest_dir.mkdir(parents=True, exist_ok=True)
        final_path = dest_dir / dest_path.name

        if final_path.exists():
            logging.info(f"Skipping existing file: {final_path}")
            with download_lock:
                download_statuses[task_id].update({
                    "status": "completed",
                    "progress": 100.0,
                    "speed": 0
                })
            return

        temp_path = dest_dir / f"{dest_path.name}.tmp"
        restricted_links = get_restricted_links(torrent_id)
        if not restricted_links:
            raise Exception("No downloadable links found")

        download_url = unrestrict_link(restricted_links[0])
        logging.info(f"Download initialized\n|-> Source: {download_url}\n|-> Temp: {temp_path}\n|-> Final: {dest_path}")

        with requests.get(download_url, stream=True, timeout=(10, 300)) as r:
            r.raise_for_status()
            total_size = int(r.headers.get("content-length", 0))
            bytes_copied = 0
            start_time = time.time()
            last_log = start_time

            with open(temp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=10*1024*1024):
                    if chunk:
                        f.write(chunk)
                        bytes_copied += len(chunk)
                        elapsed = time.time() - start_time
                        speed = bytes_copied / elapsed if elapsed > 0 else 0

                        with download_lock:
                            download_statuses[task_id].update({
                                "progress": bytes_copied / total_size if total_size > 0 else 0,
                                "speed": speed,
                                "status": "downloading"
                            })

                        if time.time() - last_log >= 3:
                            logging.info(f"[Downloading] {dest_path.name} | Progress: {bytes_copied/total_size:.1%} | Speed: {speed/1024/1024:.2f} MB/s")
                            last_log = time.time()

                f.flush()
                os.fsync(f.fileno())

        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                if temp_path.exists():
                    temp_path.rename(final_path)
                    logging.info(f"Download completed: {final_path}")
                    break
                elif final_path.exists():
                    logging.warning(f"File already exists: {final_path}")
                    break
                else:
                    if attempt == max_retries - 1:
                        raise FileNotFoundError(f"Missing both temp and final files: {temp_path}")
                    time.sleep(retry_delay)
            except FileNotFoundError as e:
                if attempt == max_retries - 1:
                    raise
                logging.warning(f"Retrying rename: {e}")
                time.sleep(retry_delay)
            except PermissionError as e:
                logging.error(f"Permission denied: {str(e)}")
                raise

        if not final_path.exists():
            raise FileNotFoundError(f"Final file missing: {final_path}")

        if MOVE_TO_FINAL_LIBRARY:
            final_lib_path = FINAL_LIBRARY_PATH / dest_path.relative_to(DOWNLOAD_COMPLETE_PATH)
            final_lib_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(final_path, final_lib_path)
            logging.info(f"Moved to final library: {final_lib_path}")
            try:
                if not any(dest_dir.iterdir()):
                    dest_dir.rmdir()
                    logging.info(f"Cleaned empty directory: {dest_dir}")
            except Exception as e:
                logging.error(f"Directory cleanup failed: {str(e)}")
        else:
            logging.info(f"File retained in downloads: {final_path}")

        time.sleep(SCAN_DELAY)
        trigger_media_scan(FINAL_LIBRARY_PATH)

        with download_lock:
            download_statuses[task_id]["status"] = "completed"

    except Exception as e:
        logging.error(f"Download failed: {str(e)}", exc_info=True)
        with download_lock:
            if task_id in download_statuses:
                download_statuses[task_id].update({
                    "status": "failed",
                    "error": str(e)
                })
        if temp_path and temp_path.exists():
            temp_path.unlink()
        raise

def get_plex_section_id():
    global plex_section_id, plex_initialized
    if plex_initialized:
        return plex_section_id
    try:
        response = requests.get(f"http://{PLEX_SERVER_IP}:32400/library/sections",
                              headers={"Accept": "application/json"},
                              params={"X-Plex-Token": PLEX_TOKEN},
                              timeout=10)
        response.raise_for_status()
        for directory in response.json()["MediaContainer"]["Directory"]:
            if directory["title"] == PLEX_LIBRARY_NAME:
                plex_section_id = str(directory["key"])
                plex_initialized = True
                logging.info(f"Plex section resolved: {plex_section_id}")
                return plex_section_id
        logging.error("Plex library missing")
        return None
    except Exception as e:
        logging.error(f"Plex error: {str(e)}")
        return None

def trigger_plex_scan(path):
    try:
        section_id = get_plex_section_id()
        if not section_id:
            return False

        if not ENABLE_DOWNLOADS:
            try:
                base_path = SYMLINK_BASE_PATH
                rel_path = path.relative_to(base_path)
                encoded_path = "/".join([urllib.parse.quote(p.name) for p in rel_path.parents[::-1]][:-1])
                params = {"path": encoded_path, "X-Plex-Token": PLEX_TOKEN}
                scan_type = "partial"
            except ValueError:
                logging.error(f"Path {path} not in symlink base {base_path}")
                params = {"X-Plex-Token": PLEX_TOKEN}
                scan_type = "full"
        else:
            params = {"X-Plex-Token": PLEX_TOKEN}
            scan_type = "full"

        response = requests.get(
            f"http://{PLEX_SERVER_IP}:32400/library/sections/{section_id}/refresh",
            params=params,
            timeout=15
        )
        logging.info(f"Plex {scan_type} scan triggered")
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Plex scan error: {str(e)}")
        return False

def trigger_emby_scan(path):
    try:
        libs_response = requests.get(f"http://{EMBY_SERVER_IP}/emby/Library/VirtualFolders?api_key={EMBY_API_KEY}",
                                   timeout=10)
        if libs_response.status_code != 200:
            return False

        library_id = None
        for lib in libs_response.json():
            if lib['Name'] == EMBY_LIBRARY_NAME:
                library_id = lib['ItemId']
                break
        if not library_id:
            return False

        scan_response = requests.post(f"http://{EMBY_SERVER_IP}/emby/Library/Refresh?api_key={EMBY_API_KEY}",
                                    timeout=15)
        return scan_response.status_code in [200, 204]
    except Exception as e:
        logging.error(f"Emby scan error: {str(e)}")
        return False

def trigger_media_scan(path):
    try:
        if MEDIA_SERVER == "plex":
            return trigger_plex_scan(path)
        elif MEDIA_SERVER == "emby":
            return trigger_emby_scan(path)
        return False
    except Exception as e:
        logging.error(f"Media scan failed: {str(e)}")
        return False

def process_symlink_creation(data, task_id):
    with download_lock:
        if task_id not in download_statuses:
            download_statuses[task_id] = {
                "status": "queued",
                "progress": 0,
                "speed": 0,
                "error": None
            }

    try:
        torrent_id = data['torrent_id']
        torrent_info = requests.get(f"https://api.real-debrid.com/rest/1.0/torrents/info/{torrent_id}",
                                  headers={"Authorization": f"Bearer {RD_API_KEY}"},
                                  timeout=15).json()

        if not torrent_info.get("files") or not torrent_info.get("filename"):
            return jsonify({"error": "Invalid torrent"}), 400

        selected_files = [f for f in torrent_info["files"] if f.get("selected") == 1]
        if not selected_files:
            return jsonify({"error": "No files selected"}), 400

        created_paths = []
        base_dir = DOWNLOAD_COMPLETE_PATH if ENABLE_DOWNLOADS else SYMLINK_BASE_PATH
        base_name = clean_filename(os.path.splitext(torrent_info["filename"])[0])
        dest_dir = base_dir / base_name
        dest_dir.mkdir(parents=True, exist_ok=True)

        for file in selected_files:
            try:
                file_path = Path(file["path"].lstrip("/"))
                # This is the name of the link in your library
                dest_path = dest_dir / f"{clean_filename(file_path.stem)}{file_path.suffix.lower()}"

                if ENABLE_DOWNLOADS:
                    log_download_speed(task_id, torrent_id, dest_path)
                else:
                    # --- 1. FILTER: Block Junk Files ---
                    if any(word in str(file_path).lower() for word in ["996gg"]):
                        logging.info(f"Skipping junk file: {file_path.name}")
                        continue 

                    # --- 2. SMART SEARCH: Find the file in /scenes ---
                    # Instead of just the messy RD name, look in your clean 'scenes' folder
                    scenes_base = RCLONE_MOUNT_PATH.parent / "scenes"
                    potential_dir = scenes_base / base_name
                    src_path = None

                    if potential_dir.exists():
                        # A. Try to find the exact file name RD gives us
                        messy_file_path = potential_dir / file_path.name
                        if messy_file_path.exists():
                            src_path = messy_file_path
                        else:
                            # B. If filename is messy (hhd800.com@...), grab the first video file
                            for entry in potential_dir.iterdir():
                                if entry.suffix.lower() in ['.mp4', '.mkv', '.avi', '.ts']:
                                    src_path = entry
                                    break
                    
                    # Fallback: If scenes search failed entirely, use the messy default
                    if not src_path:
                        src_path = RCLONE_MOUNT_PATH / torrent_info["filename"] / file_path

                    # --- 3. ROBUST LINKING: Overwrite broken/old links ---
                    try:
                        # If a link already exists (even a broken one), remove it
                        if dest_path.lexists(): 
                            dest_path.unlink()
                            
                        dest_path.symlink_to(src_path)
                        logging.info(f"Symlink created: {dest_path.name} → {src_path}")
                    except Exception as e:
                        logging.error(f"Symlink creation failed: {e}")

                    trigger_media_scan(dest_path)

                created_paths.append(str(dest_path))
            except Exception as e:
                logging.error(f"File error: {str(e)}")

        return jsonify({
            "status": "processed" if ENABLE_DOWNLOADS else "symlink_created",
            "created_paths": created_paths,
            "task_id": task_id
        }), 200
    except requests.RequestException as e:
        return jsonify({"error": "API failure"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/symlink", methods=["POST"])
def create_symlink():
    data = request.get_json()
    torrent_id = data.get("torrent_id")

    with queue_lock:
        current_torrent_ids = set(active_tasks.values())
        current_torrent_ids.update(task[2] for task in request_queue)
        if torrent_id in current_torrent_ids:
            return jsonify({"error": "Task already in progress"}), 409

    if task_semaphore.acquire(blocking=False):
        try:
            task_id = str(uuid.uuid4())
            with queue_lock:
                active_tasks[task_id] = torrent_id
            return process_symlink_creation(data, task_id)
        finally:
            task_semaphore.release()
            with queue_lock:
                if task_id in active_tasks:
                    del active_tasks[task_id]
    else:
        task_id = str(uuid.uuid4())
        with queue_lock:
            request_queue.append((task_id, request.get_data(), torrent_id))
            active_tasks[task_id] = torrent_id
        return jsonify({"status": "queued", "task_id": task_id, "position": len(request_queue)}), 429

@app.route("/task-status/<task_id>")
def get_task_status(task_id):
    with download_lock:
        status_data = download_statuses.get(task_id, {})

    compatible_status = {
        "starting": "processing",
        "downloading": "processing",
        "completed": "processed",
        "failed": "error"
    }.get(status_data.get("status"), "unknown")

    return jsonify({
        "status": compatible_status,
        "progress": status_data.get("progress", 0),
        "speed_mbps": status_data.get("speed", 0)/1024/1024,
        "filename": status_data.get("filename", ""),
    })

@app.route("/health")
def health_check():
    return jsonify({
        "status": "healthy",
        "queue_size": len(request_queue),
        "active_tasks": len(active_tasks),
        "concurrency_limit": MAX_CONCURRENT_TASKS,
        "workers_alive": sum(1 for t in workers if t.is_alive())
    }), 200

if __name__ == "__main__":
    workers = [TaskWorker() for _ in range(MAX_CONCURRENT_TASKS * 2)]
    for w in workers:
        w.start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5002")), threaded=True)
