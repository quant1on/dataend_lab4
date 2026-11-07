import os
import sys
import io
import csv
import re
import json
import time
import statistics
import threading
from datetime import datetime
from collections import defaultdict
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.client import Config
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError

sys.stdout.reconfigure(encoding="utf-8")

WORKER_ID = os.getenv("WORKER_ID", "A")
ZK_HOSTS = os.getenv("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs-home:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "hhadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "hhadmin-secret")
S3_BUCKET = os.getenv("S3_BUCKET", "hh-lake")
S3_KEY = os.getenv("S3_KEY", "it_vacancies_2024.csv")

RUN_ID = os.getenv("RUN", "demo1")
FENCING = int(os.getenv("FENCING", "0"))
SLOW = float(os.getenv("SLOW", "3"))
SESSION_TIMEOUT = float(os.getenv("SESSION_TIMEOUT", "10"))
CHUNK_SIZE_MB = int(os.getenv("CHUNK_SIZE_MB", "64"))
MAP_WORKERS = int(os.getenv("MAP_WORKERS", "3"))

VARIANT_SEGMENT = "csharp"
VARIANT_ROLE = "Программист, разработчик"
VARIANT_NAME_REGEX = re.compile(r"(?i)(c#|с#|\.net|dotnet|csharp)")

ELECTION_PATH = "/hh/election"
EPOCH_PATH = f"/hh/runs/{RUN_ID}/epoch"
RESULT_PATH = f"/hh/runs/{RUN_ID}/result"
LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, f"{RUN_ID}-{WORKER_ID}.log")

os.makedirs(LOG_DIR, exist_ok=True)
_log_lock = threading.Lock()


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"{ts} {WORKER_ID}: {msg}"
    with _log_lock:
        print(line, flush=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=5,
            read_timeout=30,
        ),
        region_name="us-east-1",
    )


class S3ChunkReader:
    def __init__(self, s3_client, bucket: str, key: str, chunk_size_bytes: int):
        self.s3 = s3_client
        self.bucket = bucket
        self.key = key
        self.chunk_size = chunk_size_bytes

        head = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        self.total_size = head["ContentLength"]

        self.chunks = []
        cur = 0
        chunk_idx = 0
        while cur < self.total_size:
            end = min(cur + self.chunk_size - 1, self.total_size - 1)
            self.chunks.append(
                {
                    "chunk_id": chunk_idx,
                    "start_byte": cur,
                    "end_byte": end,
                    "is_first": (cur == 0),
                    "is_last": (end == self.total_size - 1),
                }
            )
            cur = end + 1
            chunk_idx += 1

    def fetch_aligned_chunk(self, chunk_meta: dict) -> str:
        s3 = get_s3_client()
        start = chunk_meta["start_byte"]
        end = chunk_meta["end_byte"]
        is_first = chunk_meta["is_first"]
        is_last = chunk_meta["is_last"]

        read_start = max(0, start - 1) if not is_first else 0
        peek_extra = 65536
        read_end = min(self.total_size - 1, end + peek_extra)

        range_hdr = f"bytes={read_start}-{read_end}"
        resp = s3.get_object(Bucket=self.bucket, Key=self.key, Range=range_hdr)
        data = resp["Body"].read()

        offset = 0
        if not is_first:
            prev_byte = data[0:1]
            if prev_byte == b"\n":
                offset = 1
            else:
                idx = data.find(b"\n", 1)
                if idx != -1:
                    offset = idx + 1
                else:
                    return ""

        if is_last:
            chunk_text = data[offset:].decode("utf-8", errors="replace")
        else:
            target_cut = end - read_start
            idx = data.find(b"\n", target_cut)
            if idx != -1:
                chunk_text = data[offset : idx + 1].decode("utf-8", errors="replace")
            else:
                chunk_text = data[offset:].decode("utf-8", errors="replace")

        return chunk_text


def execute_map_task(reader: S3ChunkReader, chunk_meta: dict, slow_sec: float) -> dict:
    if slow_sec > 0:
        time.sleep(slow_sec)

    chunk_id = chunk_meta["chunk_id"]
    is_first = chunk_meta["is_first"]
    chunk_text = reader.fetch_aligned_chunk(chunk_meta)

    csv_reader = csv.reader(io.StringIO(chunk_text))

    pairs_count = defaultdict(int)
    pairs_salaries = defaultdict(list)

    rows_processed = 0
    vacancies_matched = 0
    vacancies_with_salary = 0
    header_skipped = not is_first

    for row in csv_reader:
        if not row or len(row) < 21:
            continue

        if is_first and not header_skipped:
            if row[0] == "source" and row[1] == "vacancy_id":
                header_skipped = True
                continue

        rows_processed += 1

        name = row[10]
        role = row[11]
        sal_from_str = row[13]
        sal_to_str = row[14]
        currency = row[15]
        key_skills = row[20]

        if role == VARIANT_ROLE and VARIANT_NAME_REGEX.search(name):
            vacancies_matched += 1

            salary_val = None
            if currency == "RUR" and (sal_from_str or sal_to_str):
                try:
                    sf = float(sal_from_str) if sal_from_str else None
                    st = float(sal_to_str) if sal_to_str else None
                    if sf is not None and st is not None:
                        salary_val = (sf + st) / 2.0
                    elif sf is not None:
                        salary_val = sf
                    elif st is not None:
                        salary_val = st
                except ValueError:
                    salary_val = None

            if salary_val is not None:
                vacancies_with_salary += 1

            if not key_skills or not key_skills.strip():
                continue

            raw_skills = key_skills.split(";")
            skills = sorted(list(set(s.strip() for s in raw_skills if s.strip())))
            if len(skills) < 2:
                continue

            for s1, s2 in combinations(skills, 2):
                pair_key = f"{s1} + {s2}"
                pairs_count[pair_key] += 1
                if salary_val is not None:
                    pairs_salaries[pair_key].append(salary_val)

    return {
        "chunk_id": chunk_id,
        "rows_processed": rows_processed,
        "vacancies_matched": vacancies_matched,
        "vacancies_with_salary": vacancies_with_salary,
        "pairs_count": dict(pairs_count),
        "pairs_salaries": dict(pairs_salaries),
    }


def checkpoint_key(run_id: str, chunk_id: int) -> str:
    return f"runs/{run_id}/checkpoints/task_{chunk_id:02d}.json"


def save_checkpoint(s3_client, bucket: str, run_id: str, task_result: dict):
    key = checkpoint_key(run_id, task_result["chunk_id"])
    body = json.dumps(task_result, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def load_checkpoints(s3_client, bucket: str, run_id: str) -> dict:
    prefix = f"runs/{run_id}/checkpoints/task_"
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    completed = {}
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        data = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        parsed = json.loads(data.decode("utf-8"))
        completed[int(parsed["chunk_id"])] = parsed
    return completed


def aggregate_and_format(completed_tasks: dict):
    total_rows = 0
    total_matched = 0
    total_with_salary = 0
    final_pairs_count = defaultdict(int)
    final_pairs_salaries = defaultdict(list)

    for cid in sorted(completed_tasks.keys()):
        part = completed_tasks[cid]
        total_rows += part["rows_processed"]
        total_matched += part["vacancies_matched"]
        total_with_salary += part.get("vacancies_with_salary", 0)

        for k, cnt in part["pairs_count"].items():
            final_pairs_count[k] += cnt
        for k, sal_list in part.get("pairs_salaries", {}).items():
            final_pairs_salaries[k].extend(sal_list)

    sorted_pairs = sorted(final_pairs_count.items(), key=lambda x: (-x[1], x[0]))

    rows_out = []
    total_pair_occurrences = 0
    total_salary_mentions = 0
    sum_medians = 0.0

    for pair_key, freq in sorted_pairs:
        total_pair_occurrences += freq
        sals = final_pairs_salaries.get(pair_key, [])
        sal_cnt = len(sals)
        total_salary_mentions += sal_cnt
        med = float(statistics.median(sals)) if sal_cnt > 0 else 0.0
        sum_medians += med
        rows_out.append((pair_key, freq, sal_cnt, med))

    return {
        "total_rows": total_rows,
        "total_matched": total_matched,
        "total_with_salary": total_with_salary,
        "distinct_pairs": len(sorted_pairs),
        "total_pair_occurrences": total_pair_occurrences,
        "total_salary_mentions": total_salary_mentions,
        "sum_medians": sum_medians,
        "rows": rows_out,
    }


def main():
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        pass

    s3 = get_s3_client()
    chunk_size_bytes = CHUNK_SIZE_MB * 1024 * 1024
    reader = S3ChunkReader(s3, S3_BUCKET, S3_KEY, chunk_size_bytes)
    total_tasks = len(reader.chunks)
    size_mb = round(reader.total_size / (1024 * 1024))

    log(
        f"pid {os.getpid()}; вход s3://{S3_BUCKET}/{S3_KEY}: {size_mb} МБ, "
        f"{total_tasks} map-задач, сегмент {VARIANT_SEGMENT}, SLOW={int(SLOW) if SLOW.is_integer() else SLOW}, "
        f"тайм-аут сессии {int(SESSION_TIMEOUT)} с, FENCING={FENCING}; журнал {LOG_FILE}"
    )

    state = {
        "role": "standby",
        "done_count": 0,
        "stop_heartbeat": False,
        "my_epoch": 0,
        "expected_result_version": 0,
    }

    zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)

    conn_event = threading.Event()

    def zk_listener(zk_state):
        if zk_state == KazooState.CONNECTED:
            conn_event.set()

    zk.add_listener(zk_listener)
    zk.start()
    conn_event.wait(timeout=15)
    log("соединение с ZooKeeper установлено (CONNECTED)")

    zk.ensure_path(ELECTION_PATH)
    try:
        zk.create(EPOCH_PATH, b"0", makepath=True)
    except NodeExistsError:
        pass
    try:
        zk.create(RESULT_PATH, b"", makepath=True)
    except NodeExistsError:
        pass

    def heartbeat_loop():
        while not state["stop_heartbeat"]:
            time.sleep(2.0)
            if state["stop_heartbeat"]:
                break
            if state["role"] == "leader":
                log(f"жив, лидер, готово {state['done_count']} из {total_tasks}")
            else:
                log(
                    f"жив, запасной, жду выборов, готово {state['done_count']} из {total_tasks}"
                )

    hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    hb_thread.start()

    log(f"участвую в выборах {ELECTION_PATH}")
    election = zk.Election(ELECTION_PATH, identifier=WORKER_ID)

    def leader_work():
        try:
            res_data, res_stat = zk.get(RESULT_PATH)
            if res_data and res_data.decode("utf-8").strip():
                log(
                    f"результат уже записан ({res_data.decode('utf-8').strip()}, version={res_stat.version}), выхожу"
                )
                return
            expected_res_version = res_stat.version
        except NoNodeError:
            expected_res_version = 0

        while True:
            try:
                ep_data, ep_stat = zk.get(EPOCH_PATH)
                new_epoch = int(ep_data.decode("utf-8") or "0") + 1
                zk.set(EPOCH_PATH, str(new_epoch).encode("utf-8"), version=ep_stat.version)
                break
            except BadVersionError:
                continue

        state["my_epoch"] = new_epoch
        state["expected_result_version"] = expected_res_version
        state["role"] = "leader"
        log(f"я лидер, эпоха {new_epoch}")

        s3_local = get_s3_client()
        completed_tasks = load_checkpoints(s3_local, S3_BUCKET, RUN_ID)
        state["done_count"] = len(completed_tasks)

        if state["done_count"] == 0:
            log(f"в чекпоинте готово 0 из {total_tasks}, начинаю с нуля")
        else:
            log(
                f"в чекпоинте готово {state['done_count']} из {total_tasks}, продолжаю с чекпоинта"
            )

        pending_chunks = [
            c for c in reader.chunks if c["chunk_id"] not in completed_tasks
        ]

        if pending_chunks:
            with ThreadPoolExecutor(max_workers=MAP_WORKERS) as pool:
                future_to_cid = {
                    pool.submit(execute_map_task, reader, ch, SLOW): ch["chunk_id"]
                    for ch in pending_chunks
                }
                for fut in as_completed(future_to_cid):
                    task_res = fut.result()
                    cid = task_res["chunk_id"]
                    save_checkpoint(get_s3_client(), S3_BUCKET, RUN_ID, task_res)
                    completed_tasks[cid] = task_res
                    state["done_count"] = len(completed_tasks)
                    log(
                        f"задача {cid} готова, {task_res['rows_processed']:,} записей; "
                        f"в чекпоинте {state['done_count']} из {total_tasks}"
                    )

        summary = aggregate_and_format(completed_tasks)
        log(
            f"K=2: стеков {summary['distinct_pairs']:,}, "
            f"вакансий {summary['total_matched']:,} (всего строк {summary['total_rows']:,}, "
            f"сумма частот пар {summary['total_pair_occurrences']:,}), "
            f"зарплат {summary['total_salary_mentions']:,}, "
            f"сумма медиан {summary['sum_medians']:,.1f}"
        )

        for pair_key, freq, sal_cnt, med in summary["rows"][:5]:
            log(f"    {pair_key:<35} {freq:>6,d} {sal_cnt:>6,d} {med:>8,.0f}")

        result_filename = f"result-{WORKER_ID}-e{new_epoch}.tsv"
        result_s3_key = f"runs/{RUN_ID}/{result_filename}"

        tsv_buf = io.StringIO()
        tsv_buf.write("skill_pair\tfrequency\tsalary_count\tmedian_salary\n")
        for pair_key, freq, sal_cnt, med in summary["rows"]:
            tsv_buf.write(f"{pair_key}\t{freq}\t{sal_cnt}\t{med:.1f}\n")
        tsv_bytes = tsv_buf.getvalue().encode("utf-8")

        get_s3_client().put_object(Bucket=S3_BUCKET, Key=result_s3_key, Body=tsv_bytes)

        zk_write = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
        zk_write.start()

        pointer_payload = result_filename.encode("utf-8")
        try:
            if FENCING == 1:
                _, current_ep_stat = zk_write.get(EPOCH_PATH)
                if current_ep_stat.version != new_epoch:
                    raise BadVersionError(
                        f"Epoch mismatch: expected version {new_epoch}, got {current_ep_stat.version}"
                    )
                zk_write.set(
                    RESULT_PATH, pointer_payload, version=expected_res_version
                )
            else:
                zk_write.set(RESULT_PATH, pointer_payload, version=-1)

            log(f"записал результат {result_filename} и указатель {RESULT_PATH}")
        except BadVersionError:
            log(
                f"указатель не записан, BadVersionError: моя эпоха {new_epoch} устарела, выхожу"
            )
        finally:
            try:
                zk_write.stop()
                zk_write.close()
            except Exception:
                pass

    try:
        election.run(leader_work)
    except Exception:
        pass

    state["stop_heartbeat"] = True
    log("работа закончена, закрываю сессию")
    try:
        zk.stop()
        zk.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
