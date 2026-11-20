"""
Extract structured fields (unit, category, urgency, summary) from tenant
maintenance messages with a local Docker Model Runner model and store
results in DuckDB.

Docker-hosted models:
https://hub.docker.com/u/ai
docker model pull ai/llama3.2  # 1.87 GiB
docker model pull ai/smollm2   # 256.35 MiB
"""

import csv
import json
import logging
import os

import duckdb
import httpx

API_URL = "http://127.0.0.1:12434/engines/v1/chat/completions"
DEFAULT_MODEL = "ai/llama3.2"
# DEFAULT_MODEL = "ai/smollm2"
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(HERE, "maintenance_requests.csv")
DEFAULT_DB = os.path.join(HERE, "maintenance.duckdb")
LOG_FILE = os.path.join(HERE, "maintenance.log")

KEYS = {"unit", "category", "urgency", "summary"}
CATEGORIES = ("plumbing", "electrical", "appliance", "hvac", "pest", "security", "other")
URGENCIES = ("low", "medium", "high")
UNITS = {f"{floor}{letter}" for floor in range(1, 7) for letter in "ABCD"}  # 1A..6D

logging.basicConfig(
    filename=LOG_FILE, level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("maintenance")

# No example values on purpose: small models tend to copy them (e.g. answering a sample unit).
SYSTEM_PROMPT = f"""You extract structured data from messages tenants send to building management.
Return a JSON object with exactly these keys and nothing else:
- unit: the apartment unit named in the message, written as a floor number followed by a letter.
  The building has floors 1-6 and letters A-D. Use null if the message does not name a unit
  (for example, it is about a shared area such as the lobby, elevator or laundry room).
  Never guess a unit.
- category: one of {", ".join(CATEGORIES)}. Use other for anything that is not a repair request
  (questions, complaints about neighbors, thanks, vague messages).
- urgency: one of low, medium, high.
  high = safety risk or active damage: gas smell, fire, flooding or active leak, no heat,
  sparking or exposed wires, broken lock or door, anyone locked out or at risk.
  medium = something is broken and affects daily life but is not dangerous.
  low = cosmetic, minor, can wait, or not a repair.
- summary: a short English description of the problem, about 10 words, even if the message
  is in another language. If there are several problems, mention all of them briefly."""

# JSON Schema enforced server-side by Model Runner; guarantees parseable JSON with the right keys.
RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "maintenance_request",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                # Format only (digit + letter); existence is checked in validate().
                "unit": {"anyOf": [{"type": "string", "pattern": "^[0-9][A-Da-d]$"}, {"type": "null"}]},
                "category": {"type": "string", "enum": list(CATEGORIES)},
                "urgency": {"type": "string", "enum": list(URGENCIES)},
                "summary": {"type": "string"},
            },
            "required": sorted(KEYS),
            "additionalProperties": False,
        },
    },
}


def validate(obj) -> dict:
    """Check a parsed model answer; return a cleaned copy or raise ValueError with the reason."""
    if not isinstance(obj, dict):
        raise ValueError(f"expected a JSON object, got {type(obj).__name__}")
    if set(obj) != KEYS:
        raise ValueError(f"keys must be exactly {sorted(KEYS)}, got {sorted(obj)}")
    if obj["category"] not in CATEGORIES:
        raise ValueError(f"category {obj['category']!r} not in {list(CATEGORIES)}")
    if obj["urgency"] not in URGENCIES:
        raise ValueError(f"urgency {obj['urgency']!r} not in {list(URGENCIES)}")
    if not isinstance(obj["summary"], str) or not obj["summary"].strip():
        raise ValueError("summary must be a non-empty string")
    unit = obj["unit"]
    if unit is not None:
        if not isinstance(unit, str):
            raise ValueError(f"unit must be a string or null, got {unit!r}")
        unit = unit.strip().upper()  # normalize e.g. 3c -> 3C
        if unit not in UNITS:
            raise ValueError(f"unit {unit!r} does not exist (valid units are 1A-6D); use null if no real unit is named")
    return {**obj, "unit": unit, "summary": obj["summary"].strip()}


def extract(client: httpx.Client, model: str, text: str, show_payload: bool = False) -> tuple[dict | None, str | None]:
    """Ask the model for the four fields; retry once with the error. Returns (result, error)."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": text},
    ]
    error = None
    for attempt in range(2):
        payload = {"model": model, "temperature": 0, "messages": messages,
                   "response_format": RESPONSE_FORMAT}
        if show_payload and attempt == 0:
            print(f"POST {API_URL}\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n")
        answer = None
        try:
            resp = client.post(API_URL, json=payload)
            resp.raise_for_status()
            answer = resp.json()["choices"][0]["message"]["content"]
            return validate(json.loads(answer)), None
        except httpx.ConnectError as e:
            # Server is down: abort the whole run rather than overwrite the table with NULLs.
            log.error("cannot reach %s: %s", API_URL, e)
            raise SystemExit(f"Cannot reach Model Runner at {API_URL}: {e}. Is Docker running?")
        except (httpx.HTTPError, KeyError, IndexError) as e:
            error = f"request failed: {e}"
            break  # transport problem; retrying with feedback won't help
        except (json.JSONDecodeError, ValueError) as e:
            error = f"invalid answer {answer!r}: {e}"
            log.warning("attempt %d failed: %s", attempt + 1, error)
            # Feed the bad answer and the reason back so the model can correct itself.
            messages += [
                {"role": "assistant", "content": answer or ""},
                {"role": "user", "content": f"That answer was invalid: {e}. Reply again with corrected JSON only."},
            ]
    return None, error


def main() -> None:
    try:
        with open(DEFAULT_CSV, newline="", encoding="utf-8") as f:
            records = list(csv.DictReader(f))
    except OSError as e:
        log.error("cannot read %s: %s", DEFAULT_CSV, e)
        raise SystemExit(f"Cannot read {DEFAULT_CSV}: {e}")

    # Extract fields for each message; failures are kept as NULL rows with an error reason.
    rows, failures = [], 0
    with httpx.Client(timeout=120) as client:
        for i, rec in enumerate(records):
            result, error = extract(client, DEFAULT_MODEL, rec["message"], show_payload=(i == 0))
            r = result or dict.fromkeys(KEYS)
            if error:
                failures += 1
                print(f"{rec['request_id']}: FAILED - {error}")
                log.error("%s failed: %s", rec["request_id"], error)
            else:
                print(f"{rec['request_id']}: {r['unit']} | {r['category']} | {r['urgency']} | {r['summary']}")
                log.info("%s ok: %s", rec["request_id"], json.dumps(r))
            rows.append((rec["request_id"], rec["submitted_at"], rec["tenant_name"], rec["message"],
                         r["unit"], r["category"], r["urgency"], r["summary"], error))

    # Load into DuckDB with CHECK constraints mirroring validate().
    units_sql = ", ".join(f"'{u}'" for u in sorted(UNITS))
    try:
        con = duckdb.connect(DEFAULT_DB)
        con.execute(f"""
            CREATE OR REPLACE TABLE requests (
                request_id INTEGER PRIMARY KEY,
                submitted_at TIMESTAMP,
                tenant_name VARCHAR,
                message VARCHAR,
                unit VARCHAR CHECK (unit IN ({units_sql})),
                category VARCHAR CHECK (category IN ({", ".join(f"'{c}'" for c in CATEGORIES)})),
                urgency VARCHAR CHECK (urgency IN ('low', 'medium', 'high')),
                summary VARCHAR,
                error VARCHAR
            )
        """)
        con.executemany("INSERT INTO requests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        print(f"\nWrote {len(rows)} rows ({failures} failed) to {DEFAULT_DB} (table: requests)\n")

        # Counts by urgency and category, high first; failed rows (NULL) last.
        con.sql("""
            SELECT urgency, category, count(*) AS n
            FROM requests
            GROUP BY urgency, category
            ORDER BY CASE urgency WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 ELSE 4 END,
                     n DESC, category
        """).show(max_rows=100)
        if failures:
            con.sql("SELECT request_id, error FROM requests WHERE error IS NOT NULL").show(max_width=200)
        con.close()
    except duckdb.Error as e:
        log.error("DuckDB error: %s", e)
        raise SystemExit(f"DuckDB error: {e}")


if __name__ == "__main__":
    main()
