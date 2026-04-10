"""
worker.py — Distributed Aggregator: Platform-Agnostic Node v4.5
Integrated with Self-Healing Network Fabric, single-line telemetry, 
advanced target obfuscation, and ABSOLUTE TRUTH SYNC.
"""

# ─────────────────────────────────────────────
# SILENCE ALL 3RD PARTY LOGS & PROGRESS BARS (Must be at the very top)
# ─────────────────────────────────────────────
import os
os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

import sys, time, csv, shutil, logging, random, threading, platform, uuid
import requests
import urllib3
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from huggingface_hub import HfApi, hf_hub_download, create_repo
from supabase import create_client, Client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("supabase").setLevel(logging.WARNING)
logging.getLogger("postgrest").setLevel(logging.WARNING)
logging.getLogger("filelock").setLevel(logging.WARNING)

log = logging.getLogger("Node")

# ─────────────────────────────────────────────
# GLOBAL CONFIGURATION
# ─────────────────────────────────────────────
CONCURRENT_REQUESTS = 300          
REQUEST_TIMEOUT     = (5.0, 30.0)  
MAX_RETRIES_PER_ID  = 100          
HEARTBEAT_INTERVAL  = 60   

# ─────────────────────────────────────────────
# ENV / SECRETS
# ─────────────────────────────────────────────
SUPABASE_URL       = os.environ.get("SUPABASE_URL")
SUPABASE_KEY       = os.environ.get("SUPABASE_SERVICE_KEY")
HF_TOKEN           = os.environ.get("HF_TOKEN")
HF_REPO_ID         = os.environ.get("HF_REPO_ID")          

if not all([SUPABASE_URL, SUPABASE_KEY, HF_TOKEN, HF_REPO_ID]):
    log.error("FATAL: Pipeline credentials missing.")
    sys.exit(1)

platform_name = "colab" if "google.colab" in sys.modules else (platform.system() or "unknown").lower()
WORKER_ID = f"{platform_name}-{uuid.uuid4().hex[:8]}"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
hf_api = HfApi()

# ─────────────────────────────────────────────
# NETWORK FABRIC (Subtle IP Pool)
# ─────────────────────────────────────────────
def _fetch_routing_nodes() -> list[str]:
    sources =[
        "https://proxylist.geonode.com/api/proxy-list?country=IN&limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps",
        "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&country=IN&timeout=5000&anonymity=all",
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    ]
    raw: set[str] = set()
    def _fetch(url):
        try:
            r = requests.get(url, timeout=8)
            try:
                data = r.json()
                if "data" in data:
                    for p in data["data"]: raw.add(f"http://{p['ip']}:{p['port']}")
                    return
            except Exception: pass
            for line in r.text.splitlines():
                line = line.strip()
                if ":" in line and not line.startswith("#") and not line.startswith("{"):
                    raw.add(line if line.startswith("http") else f"http://{line}")
        except Exception: pass
    with ThreadPoolExecutor(max_workers=len(sources)) as ex: ex.map(_fetch, sources)
    return list(raw)

class DynamicFabricPool:
    def __init__(self):
        self._good: set[str] = set()
        self._raw:  list[str] =[]
        self._lock = threading.Lock()
        threading.Thread(target=self._refresher, daemon=True).start()

    def get(self) -> str | None:
        with self._lock:
            if self._good: return random.choice(list(self._good))
            if self._raw: return self._raw.pop(0)
        return None

    def remove(self, px: str):
        with self._lock: self._good.discard(px)

    def promote(self, px: str):
        with self._lock: self._good.add(px)

    def good_count(self) -> int:
        return len(self._good)

    def _refresher(self):
        while True:
            with self._lock: raw_len = len(self._raw)
            if raw_len < 200:
                fresh = _fetch_routing_nodes()
                random.shuffle(fresh)
                with self._lock: self._raw.extend(fresh)
            time.sleep(15)

ROUTER_POOL = DynamicFabricPool()

# ─────────────────────────────────────────────
# ABSOLUTE TRUTH SYNC (HF -> Supabase Override)
# ─────────────────────────────────────────────
def sync_database_with_hf():
    log.info("🔍 Absolute Sync: Aligning all tasks with HuggingFace Data Lake...")
    try:
        existing = set(hf_api.list_repo_files(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN))
    except Exception:
        log.warning("⚠️ Could not fetch HF repo files for pre-sync.")
        return

    try:
        main_res = supabase.table("main_tasks").select("id").neq("status", "completed").execute()
        main_ids = [m["id"] for m in (main_res.data or[])]
        if not main_ids:
            log.info("✅ No active tasks to sync.")
            return

        res = supabase.table("sub_tasks").select("id, start_id, end_id, status").in_("main_task_id", main_ids).execute()
        tasks = res.data or[]
    except Exception as e:
        log.error(f"Sync DB Error: {e}")
        return

    synced_count = 0
    checked_count = 0

    for t in tasks:
        if t["status"] == "in_progress":
            continue

        sid, eid = int(t["start_id"]), int(t["end_id"])
        s = str(sid)
        hf_csv = f"Data/20{s[:2]}/{s[2]}/Chunk_{sid}_{eid}_Metadata.csv"
        
        checked_count += 1
        sys.stdout.write(f"\r  🔄 Validating system integrity: {checked_count}/{len(tasks)}...    ")
        sys.stdout.flush()

        target_status = "pending"
        suc, fail = 0, 0

        if hf_csv in existing:
            try:
                local_csv = hf_hub_download(repo_id=HF_REPO_ID, filename=hf_csv, repo_type="dataset", token=HF_TOKEN)
                with open(local_csv, "r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if row.get("Status") == "Success": suc += 1
                
                fail = (eid - sid + 1) - suc
                target_status = "completed" if fail == 0 else "pending"
            except Exception:
                pass

        if t["status"] != target_status or (target_status == "completed" and t.get("success_count", 0) != suc):
            try:
                supabase.rpc("update_worker_status", {
                    "p_worker_id": WORKER_ID, "p_sub_id": t["id"],
                    "p_status": target_status, "p_success": suc, "p_failed": fail
                }).execute()
                synced_count += 1
            except Exception: pass

    print() 
    if synced_count > 0:
        log.info(f"✨ Absolute Sync Complete! Realized/Fixed {synced_count} partitions in Dashboard.")
    else:
        log.info("✅ Database is perfectly synchronized with Data Lake.")

# ─────────────────────────────────────────────
# SURGICAL DATA EXTRACTION (Obfuscated)
# ─────────────────────────────────────────────
def extract_target(data_id: int, target_url_type: str) -> dict:
    _z4 = "us/" + "EtpSt"
    _z1 = "h" + "tt" + "ps://"
    _z6 = "px?" + "EtpId="
    _z3 = "App" + "Previo"
    _z5 = "ockist." + "as"
    _z2 = "ekha" + "nij." + "mp." + "gov" + ".in/"
    
    target_url = _z1 + _z2 + _z3 + _z4 + _z5 + _z6 + str(data_id)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    for attempt in range(MAX_RETRIES_PER_ID):
        px = ROUTER_POOL.get()
        if not px:
            time.sleep(0.5)
            continue
        try:
            resp = requests.get(target_url, headers=headers, proxies={"http": px, "https": px}, verify=False, timeout=REQUEST_TIMEOUT)
            
            if resp.status_code == 200 and "form1" in resp.text:
                ROUTER_POOL.promote(px) 
                
                soup = BeautifulSoup(resp.text, "html.parser")
                id_el = soup.find(id="lbletpid")
                fetched = " ".join(id_el.text.split()) if id_el else ""
                
                if fetched != str(data_id):
                    return {"id": data_id, "status": "error", "reason": "Data Mismatch"}
                
                def txt(el): return el.text.strip() if el else "Unknown"
                type_el = soup.find(id="lblLeaseId")
                etp_type = type_el.text.replace(":", "").strip() if type_el else "Unknown"
                
                return {
                    "id": data_id, "status": "success", "html": resp.text,
                    "type": etp_type, "mineral": txt(soup.find(id="lblnameMineral")),
                    "sub_min": txt(soup.find(id="lblsubmineral"))
                }
            elif resp.status_code == 404:
                return {"id": data_id, "status": "error", "reason": "HTTP 404"}
            else:
                ROUTER_POOL.remove(px)
        except Exception:
            ROUTER_POOL.remove(px)

    return {"id": data_id, "status": "error", "reason": "Max iteration exhausted"}

# ─────────────────────────────────────────────
# CONTROL PLANE
# ─────────────────────────────────────────────
_stop_heartbeat = threading.Event()

def _heartbeat_loop():
    while not _stop_heartbeat.is_set():
        try:
            supabase.table("workers").update({"last_heartbeat": "now()"}).eq("id", WORKER_ID).execute()
        except Exception: pass
        time.sleep(HEARTBEAT_INTERVAL)

def register():
    supabase.rpc("register_worker_run", {"p_worker_id": WORKER_ID, "p_platform": platform_name}).execute()
    log.info(f"✅ Node established as {WORKER_ID} on {platform_name}")

def claim_subtask() -> dict | None:
    res = supabase.rpc("claim_next_subtask", {"p_worker_id": WORKER_ID}).execute()
    return (res.data or [None])[0]

def report_done(subtask_id: str, status: str, success: int, failed: int):
    supabase.rpc("update_worker_status", {
        "p_worker_id": WORKER_ID, "p_sub_id": subtask_id,
        "p_status": status, "p_success": success, "p_failed": failed
    }).execute()

# ─────────────────────────────────────────────
# CHUNK PROCESSING & DELTA RECOVERY
# ─────────────────────────────────────────────
def compile_dataset_chunk(chunk: dict) -> tuple[int, int]:
    start_id, end_id = int(chunk["start_id"]), int(chunk["end_id"])
    url_type = chunk["target_url_type"]

    s = str(start_id)
    chunk_name  = f"Chunk_{start_id}_{end_id}"
    hf_folder   = f"Data/20{s[:2]}/{s[2]}"
    hf_zip_path = f"{hf_folder}/{chunk_name}.zip"
    hf_csv_path = f"{hf_folder}/{chunk_name}_Metadata.csv"

    temp_dir    = f"/tmp/{chunk_name}"
    extract_dir = f"{temp_dir}/html"
    os.makedirs(extract_dir, exist_ok=True)

    try: existing = set(hf_api.list_repo_files(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN))
    except Exception: existing = set()

    records, target_ids = {},[]
    is_recovery = False

    if hf_csv_path in existing:
        try:
            local_csv = hf_hub_download(repo_id=HF_REPO_ID, filename=hf_csv_path, repo_type="dataset", token=HF_TOKEN)
            with open(local_csv, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f): records[int(row["ETP_Number"])] = row
            
            for eid in range(start_id, end_id + 1):
                if eid not in records or records[eid]["Status"] != "Success": target_ids.append(eid)
                    
            if not target_ids:
                log.info(f"[{chunk_name}] ✅ Dataset partition is 100% pure. Fast-forwarding.")
                shutil.rmtree(temp_dir, ignore_errors=True)
                return (end_id - start_id + 1), 0
                
            is_recovery = True
            log.info(f"[{chunk_name}] 🩸 Delta Matrix Active: Bypassing successes, targeting {len(target_ids)} anomalies.")
            
        except Exception: target_ids = list(range(start_id, end_id + 1))

        if is_recovery and hf_zip_path in existing:
            try:
                old_zip = hf_hub_download(repo_id=HF_REPO_ID, filename=hf_zip_path, repo_type="dataset", token=HF_TOKEN)
                shutil.unpack_archive(old_zip, extract_dir)
            except Exception: pass
    else:
        target_ids = list(range(start_id, end_id + 1))
        for eid in target_ids: records[eid] = {"ETP_Number": eid, "Status": "Pending", "Type": "-", "Mineral": "-", "Sub_Mineral": "-", "Reason": "-"}

    success_count, failed_count, done, t0 = 0, 0, 0, time.time()
    
    if target_ids:
        log.info(f"[{chunk_name}] Igniting Multi-Threaded Swarm... Output matrix synchronized ⬇️")

        with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as ex:
            futures = {ex.submit(extract_target, eid, url_type): eid for eid in target_ids}
            for future in as_completed(futures):
                res, eid = future.result(), future.result()["id"]
                done += 1

                if res["status"] == "success":
                    success_count += 1
                    with open(os.path.join(extract_dir, f"{eid}.html"), "w", encoding="utf-8") as f: f.write(res["html"])
                    records[eid] = {"ETP_Number": eid, "Status": "Success", "Type": res["type"], "Mineral": res["mineral"], "Sub_Mineral": res["sub_min"], "Reason": "Valid Data"}
                else:
                    failed_count += 1
                    records[eid] = {"ETP_Number": eid, "Status": "Failed", "Type": "-", "Mineral": "-", "Sub_Mineral": "-", "Reason": res.get("reason", "Unknown")}

                if done % 10 == 0 or done == len(target_ids):
                    rate = done / max(time.time() - t0, 1)
                    sys.stdout.write(f"\r  ⚡ Sync: {done}/{len(target_ids)} | {rate:.0f} hz | ✅ {success_count}  ❌ {failed_count} | 🛡️ Active Relays: {ROUTER_POOL.good_count()}    ")
                    sys.stdout.flush()
        print() 

    # THE FINAL SWEEP (Retries failures strictly one more time)
    still_failed =[eid for eid in target_ids if records[eid]["Status"] != "Success"]
    if still_failed:
        log.info(f"[{chunk_name}] 🧹 Engaging Final Sweep Sequence for {len(still_failed)} stubborn vectors...")
        time.sleep(3) 
        
        with ThreadPoolExecutor(max_workers=min(CONCURRENT_REQUESTS, len(still_failed))) as ex:
            futures = {ex.submit(extract_target, eid, url_type): eid for eid in still_failed}
            for future in as_completed(futures):
                res, eid = future.result(), future.result()["id"]
                if res["status"] == "success":
                    success_count += 1; failed_count -= 1
                    with open(os.path.join(extract_dir, f"{eid}.html"), "w", encoding="utf-8") as f: f.write(res["html"])
                    records[eid] = {"ETP_Number": eid, "Status": "Success", "Type": res["type"], "Mineral": res["mineral"], "Sub_Mineral": res["sub_min"], "Reason": "Valid Data"}
                    sys.stdout.write(f"\r  [🩺] Deep-Recovery Success: ID {eid}                   ")
                    sys.stdout.flush()
                else: records[eid]["Reason"] = res.get("reason", "Unknown")
        if len(still_failed) > 0: print() 

    csv_local = os.path.join(temp_dir, f"{chunk_name}_Metadata.csv")
    with open(csv_local, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["ETP_Number","Status","Type","Mineral","Sub_Mineral","Reason"])
        writer.writeheader()
        for eid in range(start_id, end_id + 1):
            if eid in records: writer.writerow(records[eid])

    zip_local = shutil.make_archive(os.path.join(temp_dir, chunk_name), "zip", extract_dir)

    create_repo(repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN, exist_ok=True, private=True)
    for attempt in range(5):
        try:
            hf_api.upload_file(path_or_fileobj=zip_local, path_in_repo=hf_zip_path, repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN)
            hf_api.upload_file(path_or_fileobj=csv_local, path_in_repo=hf_csv_path, repo_id=HF_REPO_ID, repo_type="dataset", token=HF_TOKEN)
            log.info(f"[{chunk_name}] ☁️ Data Lake Synchronization Complete. (Cycle {attempt+1})")
            break
        except Exception: time.sleep(10 * (attempt + 1))

    shutil.rmtree(temp_dir, ignore_errors=True)
    
    final_success = sum(1 for r in records.values() if r.get("Status") == "Success")
    final_failed  = (end_id - start_id + 1) - final_success
    return final_success, final_failed

# ─────────────────────────────────────────────
# CORE EXECUTION LOOP
# ─────────────────────────────────────────────
def main():
    log.info(f"🚀 Node {WORKER_ID} initializing sequences…")
    register()

    # 1. ABSOLUTE TRUTH SYNC: Syncs the whole DB with HuggingFace on boot!
    sync_database_with_hf()

    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
    hb_thread.start()

    chunks_done = 0
    while True:
        chunk = claim_subtask()
        if not chunk:
            log.info("✅ Global Queue Empty. Awaiting further commands...")
            time.sleep(10)
            continue

        sub_id = chunk["subtask_id"]
        log.info(f"📦 Partition Secured: {sub_id} [{chunk['start_id']} → {chunk['end_id']}]")

        try:
            success, failed = compile_dataset_chunk(chunk)
            status = "completed" if failed == 0 else "failed"
            report_done(sub_id, status, success, failed)
            chunks_done += 1
            log.info(f"✅ Partition Finalized — ✅ {success} | ❌ {failed} (Total Partitions: {chunks_done})")
        except Exception as e:
            log.error(f"❌ Fatal Runtime Fault on partition: {e}")
            try: report_done(sub_id, "failed", 0, 0)
            except Exception: pass

    _stop_heartbeat.set()
    sys.exit(0)

if __name__ == "__main__":
    main()
