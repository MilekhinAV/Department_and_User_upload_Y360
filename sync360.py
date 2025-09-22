import csv, time, sys, json, itertools, os
from collections import defaultdict, deque
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_BASE = "https://api360.yandex.net/directory/v1"
ORG_ID = os.getenv("ORG_ID")
API_TOKEN = os.getenv("TOKEN") or os.getenv("API_TOKEN")  # Support both TOKEN and API_TOKEN

def validate_config():
    """Validate configuration - only check when actually making API calls"""
    if not ORG_ID:
        raise ValueError("ORG_ID environment variable is required")
    if not API_TOKEN:
        raise ValueError("TOKEN (or API_TOKEN) environment variable is required")

S = requests.Session()
S.headers.update({
    "Authorization": f"OAuth {API_TOKEN}",
    "Content-Type": "application/json",
})

def backoff_retry(fn, *, retries=10, base=1.0):
    for i in range(retries):
        try:
            return fn()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                time.sleep(base * (2 ** i))
                continue
            raise
    raise RuntimeError("Max retries exceeded")

def list_all_departments():
    # Пагинация: вытаскиваем все страницы
    page = 1
    per_page = 100
    result = []
    print(f"  Fetching departments from API...")
    while True:
        url = f"{API_BASE}/org/{ORG_ID}/departments?page={page}&perPage={per_page}"
        print(f"  GET {url}")
        try:
            r = backoff_retry(lambda: S.get(url))
            r.raise_for_status()
            data = r.json()
            result.extend(data.get("departments", []))
            print(f"  ✓ Retrieved {len(data.get('departments', []))} departments (page {page})")
            if page >= data.get("pages", 1):
                break
            page += 1
        except requests.HTTPError as e:
            print(f"  ❌ Failed to fetch departments: {e}")
            if e.response:
                print(f"  Response status: {e.response.status_code}")
                print(f"  Response body: {e.response.text}")
            raise
    print(f"  ✓ Total departments retrieved: {len(result)}")
    return result

def find_department_by_external_id(cache, external_id):
    # cache: list of dept dicts
    for d in cache:
        if (d.get("externalId") or "") == external_id:
            return d
    return None

def find_department_by_name(cache, name):
    # Find department by name (for cases where externalId is not set)
    for d in cache:
        if d.get("name") == name:
            return d
    return None

def ensure_department(name, external_id, parent_id=None, label=None, description=None):
    # идемпотентность: сначала попробуем найти по externalId
    existing = find_department_by_external_id(DEPT_CACHE, external_id) if external_id else None
    if existing:
        print(f"  ✓ Department {external_id} already exists")
        return existing["id"]
    
    # Если не найдено по externalId, попробуем найти по имени
    existing_by_name = find_department_by_name(DEPT_CACHE, name)
    if existing_by_name and not existing_by_name.get("externalId"):
        print(f"  ✓ Found existing department '{name}' without externalId, will update it")
        # Попробуем обновить существующий департамент, добавив externalId
        try:
            update_payload = {"externalId": external_id or ""}
            if label:
                update_payload["label"] = label
            if description:
                update_payload["description"] = description
            
            update_url = f"{API_BASE}/org/{ORG_ID}/departments/{existing_by_name['id']}"
            print(f"  Updating department {existing_by_name['id']} with externalId: {external_id}")
            
            r = backoff_retry(lambda: S.patch(update_url, data=json.dumps(update_payload)))
            r.raise_for_status()
            updated = r.json()
            print(f"  ✓ Updated department: {updated.get('id')}")
            
            # Обновим кэш
            updated["externalId"] = external_id
            for i, dept in enumerate(DEPT_CACHE):
                if dept["id"] == existing_by_name["id"]:
                    DEPT_CACHE[i] = updated
                    break
            
            return updated["id"]
        except requests.HTTPError as e:
            print(f"  ⚠️  Could not update existing department: {e}")
            print(f"  Will try to create new one...")

    payload = {
        "name": name,
        "externalId": external_id or "",
    }
    if parent_id:
        payload["parentId"] = parent_id
    if label:
        payload["label"] = label
    if description:
        payload["description"] = description

    url = f"{API_BASE}/org/{ORG_ID}/departments"
    print(f"  Creating department: {name} (external_id: {external_id})")
    
    try:
        r = backoff_retry(lambda: S.post(url, data=json.dumps(payload)))
        if r.status_code == 409:
            # параллельный ран/существует — перечитать кэш
            print(f"  Department {external_id} already exists (409)")
            refresh_dept_cache()
            existing = find_department_by_external_id(DEPT_CACHE, external_id)
            if existing:
                return existing["id"]
            r.raise_for_status()
        r.raise_for_status()
        created = r.json()
        print(f"  ✓ Created department: {created.get('id')}")
        # обновим кэш локально
        created["externalId"] = external_id
        DEPT_CACHE.append(created)
        return created["id"]
    except requests.HTTPError as e:
        print(f"  ❌ Failed to create department {external_id}: {e}")
        if e.response:
            print(f"  Response status: {e.response.status_code}")
            print(f"  Response body: {e.response.text}")
        raise

def build_hierarchy_and_create(dept_rows):
    # Проверка на циклы и сортировка (Kahn’s algorithm)
    by_ext = {r["external_id"]: r for r in dept_rows}
    graph = defaultdict(list)
    indeg = defaultdict(int)

    for r in dept_rows:
        pe = r.get("parent_external_id") or None
        if pe and pe in by_ext:
            graph[pe].append(r["external_id"])
            indeg[r["external_id"]] += 1
        else:
            indeg.setdefault(r["external_id"], 0)

    q = deque([k for k, v in indeg.items() if v == 0])
    order = []
    while q:
        u = q.popleft()
        order.append(u)
        for v in graph.get(u, []):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)

    if len(order) != len(dept_rows):
        raise ValueError("Обнаружен цикл в иерархии подразделений")

    # Создание в порядке top-down
    ext_to_id = {}
    for ext in order:
        r = by_ext[ext]
        parent_id = None
        pe = r.get("parent_external_id") or None
        if pe:
            parent_id = ext_to_id.get(pe)
            if pe and parent_id is None and pe in by_ext:
                # safety: родитель должен уже быть создан
                raise RuntimeError(f"Родитель {pe} не создан к моменту создания {ext}")
        dep_id = ensure_department(
            name=r["name"],
            external_id=r["external_id"],
            parent_id=parent_id,
            label=r.get("label") or None,
            description=r.get("description") or None
        )
        ext_to_id[ext] = dep_id
    return ext_to_id

def refresh_dept_cache():
    global DEPT_CACHE
    DEPT_CACHE = list_all_departments()

def create_user(u, ext_to_id):
    dept_id = ext_to_id[u["dept_external_id"]]
    body = {
        "nickname": u["nickname"],
        "departmentId": dept_id,
        "name": {
            "first": u["first"],
            "last": u["last"],
            "middle": u.get("middle", "")
        },
        "position": u.get("position", ""),
        "language": u.get("language", "ru"),
        "timezone": u.get("timezone", "Europe/Moscow"),
        "externalId": u.get("externalId", ""),
        "password": u.get("password", ""),
        "passwordChangeRequired": str(u.get("passwordChangeRequired", "true")).lower() == "true"
    }
    url = f"{API_BASE}/org/{ORG_ID}/users"
    r = backoff_retry(lambda: S.post(url, data=json.dumps(body)))
    if r.status_code == 409:
        # Уже существует пользователь с таким nickname — логика по месту:
        return {"status": "exists", "nickname": u["nickname"]}
    r.raise_for_status()
    return {"status": "created", "nickname": u["nickname"], "id": r.json().get("id")}

def load_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding="utf-8") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):  # Start at 2 because header is row 1
                clean_row = {(k.strip() if k else k): (v.strip() if isinstance(v, str) and v else v) for k, v in row.items()}
                rows.append(clean_row)
    except FileNotFoundError:
        raise FileNotFoundError(f"CSV file not found: {path}")
    except Exception as e:
        raise Exception(f"Error reading CSV file {path}: {e}")
    return rows

def validate_departments_csv(dept_rows):
    """Validate departments CSV data"""
    required_fields = ["external_id", "name"]
    for i, row in enumerate(dept_rows, start=2):
        for field in required_fields:
            if not row.get(field):
                raise ValueError(f"Row {i}: Missing required field '{field}'")
    
    # Check for duplicate external_ids
    external_ids = [row["external_id"] for row in dept_rows]
    duplicates = set([x for x in external_ids if external_ids.count(x) > 1])
    if duplicates:
        raise ValueError(f"Duplicate external_id values found: {duplicates}")

def validate_users_csv(user_rows):
    """Validate users CSV data"""
    required_fields = ["nickname", "first", "last", "dept_external_id"]
    for i, row in enumerate(user_rows, start=2):
        for field in required_fields:
            if not row.get(field):
                raise ValueError(f"Row {i}: Missing required field '{field}'")
    
    # Check for duplicate nicknames
    nicknames = [row["nickname"] for row in user_rows]
    duplicates = set([x for x in nicknames if nicknames.count(x) > 1])
    if duplicates:
        raise ValueError(f"Duplicate nickname values found: {duplicates}")

def dry_run():
    """Perform a dry run without making API calls"""
    print("🔍 DRY RUN MODE - No API calls will be made")
    print("=" * 50)
    
    # Load and validate CSV data
    print("Loading and validating CSV data...")
    dept_rows = load_csv("departments.csv")
    user_rows = load_csv("users.csv")
    
    validate_departments_csv(dept_rows)
    validate_users_csv(user_rows)
    print(f"✓ Loaded {len(dept_rows)} departments and {len(user_rows)} users")
    
    # Show what would be created
    print("\n📁 Departments that would be created:")
    for dept in dept_rows:
        parent = dept.get('parent_external_id', 'None')
        print(f"  • {dept['external_id']} ({dept['name']}) -> parent: {parent}")
    
    print("\n👥 Users that would be created:")
    for user in user_rows:
        print(f"  • {user['nickname']} ({user['first']} {user['last']}) -> dept: {user['dept_external_id']}")
    
    print(f"\n✅ Dry run completed successfully!")
    print(f"   - {len(dept_rows)} departments would be processed")
    print(f"   - {len(user_rows)} users would be processed")

if __name__ == "__main__":
    import sys
    
    # Check for dry run mode
    if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
        dry_run()
        sys.exit(0)
    
    try:
        print("Starting Yandex 360 sync process...")
        
        # 1) Load and validate CSV data first
        print("Loading and validating CSV data...")
        dept_rows = load_csv("departments.csv")
        user_rows = load_csv("users.csv")
        
        validate_departments_csv(dept_rows)
        validate_users_csv(user_rows)
        print(f"✓ Loaded {len(dept_rows)} departments and {len(user_rows)} users")
        
        # 2) Validate configuration and initialize department cache
        print("Validating configuration...")
        validate_config()
        
        print("Initializing department cache...")
        DEPT_CACHE = []
        refresh_dept_cache()

        # 3) Create departments
        print("Creating departments...")
        ext_to_id = build_hierarchy_and_create(dept_rows)
        print(f"✓ Created/verified {len(ext_to_id)} departments")

        # 4) Refresh cache after department creation
        refresh_dept_cache()

        # 5) Create users
        print("Creating users...")
        results = []
        for u in user_rows:
            if u["dept_external_id"] not in ext_to_id:
                raise ValueError(f"Department not found: {u['dept_external_id']} for user {u['nickname']}")
            res = create_user(u, ext_to_id)
            results.append(res)
            # Small delay between user creations to avoid overwhelming the API
            time.sleep(0.5)

        # 6) Output results
        print("\n=== RESULTS ===")
        print(json.dumps(results, ensure_ascii=False, indent=2))
        
        # Summary
        created = len([r for r in results if r.get("status") == "created"])
        exists = len([r for r in results if r.get("status") == "exists"])
        print(f"\n✓ Summary: {created} users created, {exists} users already existed")
        
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)
