import hashlib
import json
import logging
import mimetypes
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
import asyncio
from pathlib import Path
from typing import List, Optional, Tuple, Set, Dict, Any
import asyncpg
import jwt
from dotenv import load_dotenv
from fastapi import FastAPI, APIRouter, HTTPException, WebSocket, WebSocketDisconnect, Depends, status
from fastapi import UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials, HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import zipfile
import io
import aiofiles
import aiofiles.os as aos

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)
UPLOAD_DIR = Path(__file__).parent.resolve() / "uploads"
app = FastAPI(title="TestWatch")
api_router = APIRouter(prefix="/api")
DB_HOST = os.getenv("POSTGRES_DB", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB_NAME", "tw")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
db_pool = None


async def get_db_connection():
    global db_pool
    if db_pool is None:
        try:
            db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                min_size=1,
                max_size=10)
        except Exception as e:
            logger.error(f"Connect PostgreSQL error: {str(e)}", exc_info=False)
            exit(1)
    return db_pool


EXPECTED_USERNAME = os.environ.get("APP_USERNAME")
EXPECTED_PASSWORD = os.environ.get("APP_PASSWORD")
SECRET_KEY = os.environ.get("SECRET_KEY")
security = HTTPBasic()
bearer_scheme = HTTPBearer()


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._heartbeat_tasks: Dict[WebSocket, asyncio.Task] = {}
        self._last_pong_ms: Dict[WebSocket, int] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        task = asyncio.create_task(self._heartbeat(websocket))
        self._heartbeat_tasks[websocket] = task

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        task = self._heartbeat_tasks.pop(websocket, None)
        if task and not task.done():
            task.cancel()
        self._last_pong_ms.pop(websocket, None)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                try:
                    await connection.close()
                except Exception:
                    pass
                self.disconnect(connection)

    async def _heartbeat(self, websocket: WebSocket):
        try:
            while True:
                await asyncio.sleep(60)
                try:
                    await websocket.send_json(
                        {"type": "ping", "ts": int(datetime.now(timezone.utc).timestamp() * 1000)})
                except Exception:
                    self.disconnect(websocket)
                    break
        except asyncio.CancelledError:
            pass

    @property
    def last_pong_ms(self):
        return self._last_pong_ms


manager = ConnectionManager()
SYNC_LOCK = asyncio.Lock()


class TestPlanBase(BaseModel):
    test_plan_id: Optional[str] = None
    name: str
    env: str
    tests: Optional[List[str]] = None
    created_at: Optional[str] = None


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str


class StatusCount(BaseModel):
    status: str
    count: int


class TestPlanList(BaseModel):
    test_plan_id: str
    name: str
    env: str
    created_at: str
    statuses: List[StatusCount]


class TestPlanListResponse(BaseModel):
    test_plans: List[TestPlanList]
    total_count: int
    page: int
    size: int


class TestRunResponse(BaseModel):
    test_plan_id: str
    test_run_id: str
    date_start: str
    params_testrun: Optional[List[Dict[str, Any]]] = None


class TestStep(BaseModel):
    name: str
    status: str
    steps: Optional[List['TestStep']] = []


class FileReference(BaseModel):
    file_id: str
    filename: str


class Link(BaseModel):
    name: Optional[str]
    url: str
    type: Optional[str] = None


class TestResult(BaseModel):
    test_id: str
    name: str
    epic: Optional[str] = None
    story: Optional[str] = None
    feature: Optional[str] = None
    test_run_id: str
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    status: str
    stacktrace: Optional[str] = None
    message: Optional[str] = None
    number: int
    steps: List[TestStep]
    links: Optional[List[Link]] = None
    files: Optional[List[FileReference]] = None
    params: Optional[List[Dict[str, Any]]] = None
    params_test: Optional[List[Dict[str, Any]]] = None
    tags: Optional[List[str]] = None


class TestPlanDetail(BaseModel):
    test_plan_id: str
    name: str
    env: str
    created_at: str
    results: List[TestResult]


class Test(BaseModel):
    test_id: str
    name: Optional[str] = None
    epic: Optional[str] = None
    story: Optional[str] = None
    feature: Optional[str] = None
    tags: List[str] = None
    template: Optional[bool] = None
    tms_links: Optional[List[str]] = None
    params_test: Optional[List[Dict[str, Any]]] = None


class TestHistoryItem(BaseModel):
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    status: str
    message: Optional[str] = None
    number: int
    test_plan_id: str
    test_plan_name: str
    test_plan_env: str
    params: Optional[List[Dict[str, Any]]] = None


class TestHistoryResponse(BaseModel):
    test_id: str
    history: List[TestHistoryItem]
    total: int


class TestRunHistoryItem(BaseModel):
    test_run_id: str
    date_start: str
    statuses: List[StatusCount]
    params_testrun: Optional[List[Dict[str, Any]]] = None


class TestPlanMetrics(BaseModel):
    test_plan_id: str
    total_test_runs_duration_sec: int
    test_plan_duration_sec: int


class PrometheusTestMetric(BaseModel):
    test_id: str
    test_name: str
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    status: str
    status_code: int


def get_status_code(status_name: str) -> int:
    status_mapping = {
        'failed': 0,
        'passed': 1,
        'started': 2,
        'waiting': 3,
        'skipped': 4
    }
    return status_mapping.get(status_name.lower(), 5)


def convert_to_snake_case(name: str) -> str:
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def extract_test_identifier(test_id: str) -> str:
    import re
    patterns = [
        r'\.([A-Z][a-zA-Z0-9]*)#([a-zA-Z][a-zA-Z0-9]*)',
        r'\.([A-Z][a-zA-Z0-9]*)\.([a-zA-Z][a-zA-Z0-9]*)'
    ]
    for pattern in patterns:
        match = re.search(pattern, test_id)
        if match:
            class_name = match.group(1)
            method_name = match.group(2)
            class_snake = convert_to_snake_case(class_name)
            method_snake = convert_to_snake_case(method_name)
            return f"{class_snake}_{method_snake}"
    last_part = test_id.split('.')[-1]
    if '#' in last_part:
        last_part = last_part.split('#')[0]
    return convert_to_snake_case(last_part)


def format_prometheus_metrics(metrics: List[PrometheusTestMetric]) -> str:
    lines = [
        "# HELP test_status Execution status of automated tests (0=failed, 1=passed, 2=started, 3=waiting, 4=skipped)",
        "# TYPE test_status gauge"]
    for metric in metrics:
        test_identifier = extract_test_identifier(metric.test_id)
        metric_name = f"test_status"
        lines.append(f'{metric_name}{{test_name="{test_identifier}"}} {metric.status_code}')
    return '\n'.join(lines)


class CreateTestRunRequest(BaseModel):
    params_testrun: Optional[List[Dict[str, Any]]] = None


class DeleteTestResultsRequest(BaseModel):
    test_ids: List[str]


class TestRunTestRequest(BaseModel):
    test: Test
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    status: str
    stacktrace: Optional[str] = None
    message: Optional[str] = None
    number: Optional[str] = 0
    steps: Optional[List[TestStep]] = None
    links: Optional[List[Link]] = None
    file_ids: Optional[List[str]] = None
    params: Optional[List[Dict[str, Any]]] = None


TestStep.model_rebuild()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return hashlib.sha256(plain_password.encode()).hexdigest() == hashed_password


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def authenticate_user(username: str, password: str) -> bool:
    hashed_input_password = hash_password(password)
    expected_hashed_password = hash_password(EXPECTED_PASSWORD)
    return username == EXPECTED_USERNAME and hashed_input_password == expected_hashed_password


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    hashed_input_password = hash_password(credentials.password)
    expected_hashed_password = hash_password(EXPECTED_PASSWORD)
    correct_username = secrets.compare_digest(credentials.username, EXPECTED_USERNAME)
    correct_password = secrets.compare_digest(hashed_input_password, expected_hashed_password)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=30)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm="HS256")
    return encoded_jwt


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return username
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def init_database():
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS TestPlan (
                test_plan_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                env TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS TestRun (
                test_run_id TEXT PRIMARY KEY,
                test_plan_id TEXT NOT NULL,
                date_start TEXT NOT NULL,
                params_testrun TEXT
            );
            CREATE TABLE IF NOT EXISTS Test (
                test_id TEXT PRIMARY KEY,
                name TEXT,
                epic TEXT,
                story TEXT,
                feature TEXT,
                deleted BOOLEAN NOT NULL DEFAULT FALSE,
                template BOOLEAN,
                tms_links TEXT,
                params_test TEXT,
                tags TEXT
            );
            CREATE TABLE IF NOT EXISTS Status (
                status_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS TestRunTests (
                test_run_id TEXT NOT NULL,
                test_id TEXT NOT NULL,
                date_start TEXT,
                date_end TEXT,
                status_id INTEGER NOT NULL,
                stacktrace TEXT,
                message TEXT,
                number INTEGER,
                steps TEXT,
                file_ids TEXT,
                links TEXT,
                params TEXT,
                PRIMARY KEY (test_run_id, test_id, number),
                FOREIGN KEY (test_run_id) REFERENCES TestRun (test_run_id) ON DELETE CASCADE,
                FOREIGN KEY (test_id) REFERENCES Test (test_id) ON DELETE CASCADE,
                FOREIGN KEY (status_id) REFERENCES Status (status_id) ON DELETE RESTRICT
            );
            CREATE TABLE IF NOT EXISTS TestRunFiles (
                file_id TEXT PRIMARY KEY,
                test_run_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                FOREIGN KEY (test_run_id) REFERENCES TestRun(test_run_id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS Settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                is_update BOOLEAN NOT NULL DEFAULT FALSE,
                date_update_tests TEXT
            );
        """)
        await conn.execute(
            "INSERT INTO Settings (id, is_update, date_update_tests) VALUES (1, FALSE, NULL) ON CONFLICT DO NOTHING")
        await conn.execute("""
            ALTER TABLE TestPlan ADD COLUMN IF NOT EXISTS created_at TEXT;
        """)
        await conn.execute("""
            UPDATE TestPlan 
            SET created_at = COALESCE(
                (SELECT MIN(tr.date_start) FROM TestRun tr WHERE tr.test_plan_id = TestPlan.test_plan_id),
                '1970-01-01T00:00:00+00:00'
            )
            WHERE created_at IS NULL;
        """)
        await conn.execute("""
            ALTER TABLE TestPlan ALTER COLUMN created_at SET NOT NULL;
        """)
        await conn.execute("""
            ALTER TABLE Test ADD COLUMN IF NOT EXISTS tags TEXT;
        """)
        default_statuses = ['waiting', 'started', 'passed', 'failed', 'skipped']
        for status_name in default_statuses:
            await conn.execute("INSERT INTO Status (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", status_name)


@api_router.post("/test/sync", dependencies=[Depends(verify_token)])
async def sync_tests(tests: List[Test]):
    async with SYNC_LOCK:
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE Test SET deleted = TRUE")
                for test in tests:
                    existing_test = await conn.fetchrow(
                        "SELECT test_id FROM Test WHERE test_id = $1",
                        test.test_id
                    )
                    tags_json = json.dumps(test.tags) if getattr(test, 'tags', None) else None
                    if existing_test:
                        tms_links_json = json.dumps(test.tms_links) if test.tms_links else None
                        params_test_json = json.dumps(test.params_test) if getattr(test, 'params_test', None) else None
                        await conn.execute("""
                                UPDATE Test 
                                SET name = $1, epic = $2, story = $3, feature = $4, deleted = FALSE, template = $5, tms_links = $6, params_test = $7, tags = $8
                                WHERE test_id = $9
                            """,
                                           test.name,
                                           test.epic,
                                           test.story,
                                           test.feature,
                                           test.template,
                                           tms_links_json,
                                           params_test_json,
                                           tags_json,
                                           test.test_id
                                           )
                    else:
                        tms_links_json = json.dumps(test.tms_links) if test.tms_links else None
                        params_test_json = json.dumps(test.params_test) if getattr(test, 'params_test', None) else None
                        await conn.execute("""
                                INSERT INTO Test (test_id, name, epic, story, feature, deleted, template, tms_links, params_test, tags)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                            """,
                                           test.test_id,
                                           test.name,
                                           test.epic,
                                           test.story,
                                           test.feature,
                                           False,
                                           test.template,
                                           tms_links_json,
                                           params_test_json,
                                           tags_json
                                           )
                await conn.execute(
                    "UPDATE Settings SET is_update = false, date_update_tests = $1 WHERE id = 1",
                    datetime.now(timezone.utc).isoformat()
                )
            await manager.broadcast({"action": "tests-updated"})
    return {"status": "success", "message": f"Synced {len(tests)} tests"}


@api_router.post("/login", response_model=LoginResponse)
async def login(credentials: LoginRequest):
    if not authenticate_user(credentials.username, credentials.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    access_token_expires = timedelta(hours=30)
    access_token = create_access_token(
        data={"sub": credentials.username}, expires_delta=access_token_expires
    )
    return LoginResponse(
        access_token=access_token,
        token_type="bearer"
    )

@api_router.get("/version", response_model=Dict[str, str])
async def get_version() -> Dict[str, str]:
    return {"version": "1.5.0"}


@api_router.get("/info", dependencies=[Depends(verify_token)])
async def info_get():
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_update, date_update_tests FROM Settings WHERE id = 1")
        return {
            "update": bool(row["is_update"]),
            "date_update_tests": row["date_update_tests"]
        }
    return None


@api_router.post("/sync", dependencies=[Depends(verify_token)])
async def info_post():
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE Settings SET is_update = true WHERE id = 1")
    await manager.broadcast({"action": "tests-updated"})
    return {"status": "success"}


@api_router.post("/testrun/{test_run_id}/file", dependencies=[Depends(verify_token)])
async def upload_file(test_run_id: str, file: UploadFile = File(...)):
    if file.size is not None and file.size > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File too large")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM TestRun WHERE test_run_id = $1", test_run_id)
        if not exists:
            raise HTTPException(status_code=404, detail="Test run not found")
    file_id = str(uuid.uuid4())
    upload_dir = UPLOAD_DIR / test_run_id
    file_path = upload_dir / file_id
    try:
        upload_dir.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(file_path, "wb") as f:
            while True:
                chunk = await file.read(8192)
                if not chunk:
                    break
                await f.write(chunk)
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO TestRunFiles (file_id, test_run_id, filename)
                VALUES ($1, $2, $3)
            """, file_id, test_run_id, file.filename)
    except (PermissionError, OSError) as e:
        if file_path.exists():
            try:
                await aos.remove(file_path)
            except Exception:
                pass
        logger.error(f"File system error: {e}")
        raise HTTPException(status_code=500, detail="File system error")
    except asyncpg.PostgresError as e:
        if file_path.exists():
            try:
                await aos.remove(file_path)
            except Exception:
                pass
        logger.error(f"Database error: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    return {"file_id": file_id}


@api_router.post("/testplan", dependencies=[Depends(verify_token)])
async def create_test_plan(request: TestPlanBase):
    test_plan_id = str(uuid.uuid4())
    test_run_id = str(uuid.uuid4())
    date_start = datetime.now(timezone.utc).isoformat()
    created_at = datetime.now(timezone.utc).isoformat()
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await conn.execute("""
                    INSERT INTO TestPlan (test_plan_id, name, env, created_at)
                    VALUES ($1, $2, $3, $4)
                """, test_plan_id, request.name, request.env.upper(), created_at)
                if request.tests:
                    row = await conn.fetchrow("SELECT status_id FROM Status WHERE name = $1", "waiting")
                    if not row:
                        raise HTTPException(status_code=500, detail="Status 'waiting' not found")
                    waiting_status_id = row["status_id"]
                    placeholders = ', '.join(f'${i}' for i in range(1, len(request.tests) + 1))
                    query = f"""
                        SELECT test_id, template, tms_links, params_test 
                        FROM Test 
                        WHERE test_id IN ({placeholders})
                    """
                    test_rows = await conn.fetch(query, *request.tests)
                    testrun_tests_data = []
                    for row in test_rows:
                        test_id = row["test_id"]
                        number = 1 if row["template"] else 0
                        testrun_tests_data.append((test_run_id, test_id, waiting_status_id, number, date_start))
                    if testrun_tests_data:
                        await conn.execute("""
                            INSERT INTO TestRun (test_run_id, test_plan_id, date_start)
                            VALUES ($1, $2, $3)
                        """, test_run_id, test_plan_id, date_start)
                        for tr_test in testrun_tests_data:
                            await conn.execute("""
                                INSERT INTO TestRunTests (test_run_id, test_id, status_id, number, date_start)
                                VALUES ($1, $2, $3, $4, $5)
                            """, *tr_test)
                status_rows = await conn.fetch("SELECT name FROM Status ORDER BY name")
                statuses = [
                    {"status": row["name"], "count": 0}
                    for row in status_rows
                ]
            except asyncpg.IntegrityConstraintViolationError as e:
                raise HTTPException(status_code=400, detail=f"Test plan could not be created: {str(e)}")
            except asyncpg.PostgresError as e:
                raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    return {
        "test_plan_id": test_plan_id,
        "name": request.name,
        "env": request.env,
        "created_at": created_at,
        "status": "created",
        "statuses": statuses
    }


@api_router.get("/testplan", response_model=TestPlanListResponse, dependencies=[Depends(verify_token)])
async def get_testplans(page: int = 1, size: int = 5, name_filter: str = None):
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be >= 1")
    if size < 1 or size > 100:
        raise HTTPException(status_code=400, detail="Size must be between 1 and 100")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        filter_clause = ""
        filter_args = []
        arg_offset = 1
        if name_filter:
            filter_clause = "WHERE LOWER(name) LIKE $1"
            filter_args = [f"%{name_filter.lower()}%"]
            arg_offset = 2
        count_query = f"SELECT COUNT(*) FROM TestPlan {filter_clause}"
        total_count = await conn.fetchval(count_query, *filter_args)
        if total_count == 0:
            return TestPlanListResponse(test_plans=[], total_count=0, page=page, size=size)
        offset = (page - 1) * size
        select_query = f"""
            SELECT test_plan_id, name, env, created_at 
            FROM TestPlan 
            {filter_clause}
            ORDER BY created_at DESC 
            LIMIT ${arg_offset} OFFSET ${arg_offset + 1}
        """
        testplan_rows = await conn.fetch(select_query, *filter_args, size, offset)
        if not testplan_rows:
            return TestPlanListResponse(test_plans=[], total_count=total_count, page=page, size=size)
        plan_ids = [row["test_plan_id"] for row in testplan_rows]
        placeholders = ", ".join(f"${i + 1}" for i in range(len(plan_ids)))
        main_args = plan_ids
        main_query = f"""
        WITH LatestTestRunTests AS (
            SELECT 
                trt.test_run_id,
                trt.test_id,
                trt.number,
                trt.status_id,
                tr.test_plan_id,
                ROW_NUMBER() OVER (
                    PARTITION BY tr.test_plan_id, trt.test_id, trt.number 
                    ORDER BY tr.date_start DESC
                ) AS rn
            FROM TestRun tr
            JOIN TestRunTests trt ON tr.test_run_id = trt.test_run_id
            WHERE tr.test_plan_id IN ({placeholders})
        ),
        LatestStatuses AS (
            SELECT 
                l.test_plan_id,
                s.name AS status_name
            FROM LatestTestRunTests l
            JOIN Status s ON l.status_id = s.status_id
            WHERE l.rn = 1
        ),
        StatusCounts AS (
            SELECT 
                test_plan_id,
                status_name,
                COUNT(*) AS cnt
            FROM LatestStatuses
            GROUP BY test_plan_id, status_name
        ),
        AllStatuses AS (
            SELECT DISTINCT name AS status_name FROM Status
        ),
        PlanStatuses AS (
            SELECT 
                tp.test_plan_id,
                as_.status_name,
                COALESCE(sc.cnt, 0) AS count
            FROM (SELECT test_plan_id FROM TestPlan WHERE test_plan_id IN ({placeholders})) tp
            CROSS JOIN AllStatuses as_
            LEFT JOIN StatusCounts sc 
                ON sc.test_plan_id = tp.test_plan_id 
                AND sc.status_name = as_.status_name
        )
        SELECT 
            ps.test_plan_id,
            tp.name,
            tp.env,
            tp.created_at,
            ps.status_name,
            ps.count
        FROM PlanStatuses ps
        JOIN TestPlan tp ON ps.test_plan_id = tp.test_plan_id
        ORDER BY ps.test_plan_id, ps.status_name
        """
        rows = await conn.fetch(main_query, *main_args)
    from collections import defaultdict
    plan_data = defaultdict(dict)
    plan_info = {}
    for row in rows:
        plan_id = row["test_plan_id"]
        plan_info[plan_id] = {
            "name": row["name"],
            "env": row["env"],
            "created_at": row["created_at"]
        }
        plan_data[plan_id][row["status_name"]] = row["count"]
    all_status_names = sorted({row["status_name"] for row in rows}) if rows else []
    test_plans = []
    for row in testplan_rows:
        plan_id = row["test_plan_id"]
        counts = plan_data.get(plan_id, {})
        statuses = [
            StatusCount(status=st, count=counts.get(st, 0))
            for st in all_status_names
        ]
        test_plans.append(TestPlanList(
            test_plan_id=plan_id,
            name=row["name"],
            env=row["env"],
            created_at=row["created_at"],
            statuses=statuses
        ))
    return TestPlanListResponse(
        test_plans=test_plans,
        total_count=total_count,
        page=page,
        size=size
    )


@api_router.get("/testplan/{test_plan_id}", response_model=TestPlanDetail, dependencies=[Depends(verify_token)])
async def get_testplan_detail(test_plan_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        plan_row = await conn.fetchrow(
            "SELECT test_plan_id, name, env, created_at FROM TestPlan WHERE test_plan_id = $1",
            test_plan_id
        )
        if not plan_row:
            raise HTTPException(status_code=404, detail="Test plan not found")
        rows = await conn.fetch("""
        WITH LatestTestRunTests AS (
            SELECT 
                trt.test_id,
                trt.number,
                trt.test_run_id,
                tr.date_start,
                ROW_NUMBER() OVER (
                    PARTITION BY trt.test_id, trt.number 
                    ORDER BY tr.date_start DESC
                ) AS rn
            FROM TestRun tr
            JOIN TestRunTests trt ON tr.test_run_id = trt.test_run_id
            WHERE tr.test_plan_id = $1
        )
        SELECT 
            trt.test_id, 
            t.name,
            t.epic,
            t.story,
            t.feature,
            trt.test_run_id, 
            trt.date_start, 
            trt.date_end, 
            s.name AS status, 
            trt.stacktrace, 
            trt.message, 
            trt.number, 
            trt.steps,
            (
                SELECT STRING_AGG(f2.file_id || '::' || f2.filename, ',')
                FROM TestRunFiles f2
        WHERE f2.test_run_id = trt.test_run_id
          AND f2.file_id = ANY(
              ARRAY(
                  SELECT json_array_elements_text(
                      CASE 
                          WHEN trt.file_ids IS NULL OR trt.file_ids = '' OR trt.file_ids = '[]'
                          THEN '[]'::json
                          ELSE trt.file_ids::json
                      END
                  )
              )
          )
            ) AS files,
            trt.links,
            trt.params,
            t.params_test,
            t.tags
        FROM TestRunTests trt
        JOIN LatestTestRunTests l 
            ON trt.test_run_id = l.test_run_id
            AND trt.test_id = l.test_id
            AND trt.number = l.number
        JOIN Test t ON trt.test_id = t.test_id
        JOIN Status s ON trt.status_id = s.status_id
        WHERE l.rn = 1
        GROUP BY trt.test_id, trt.number, trt.test_run_id, t.name, t.epic, t.story, t.feature,
                 trt.date_start, trt.date_end, s.name, trt.stacktrace, trt.message, trt.steps,
                 trt.links, trt.params, t.params_test, t.tags
        ORDER BY trt.test_id, trt.number
        """, test_plan_id)
        results = []
        for row in rows:
            steps = json.loads(row["steps"]) if row["steps"] else []
            links = json.loads(row["links"]) if row["links"] else []
            _params_raw = json.loads(row["params"]) if row["params"] else None
            if isinstance(_params_raw, dict):
                params = [_params_raw]
            elif isinstance(_params_raw, list):
                params = _params_raw
            else:
                params = None
            params_test = json.loads(row["params_test"]) if row["params_test"] else None
            tags = []
            if row["tags"]:
                try:
                    tags = json.loads(row["tags"])
                except Exception:
                    tags = []
            files = []
            if row["files"]:
                for part in row["files"].split(','):
                    if '::' in part:
                        fid, fname = part.split('::', 1)
                        files.append({"file_id": fid, "filename": fname})
            results.append(TestResult(
                test_id=row["test_id"],
                name=row["name"],
                epic=row["epic"],
                story=row["story"],
                feature=row["feature"],
                test_run_id=row["test_run_id"],
                date_start=row["date_start"],
                date_end=row["date_end"],
                status=row["status"],
                stacktrace=row["stacktrace"],
                message=row["message"],
                number=row["number"],
                steps=steps,
                files=files,
                links=links,
                params=params,
                params_test=params_test,
                tags=tags,
            ))
    return TestPlanDetail(
        test_plan_id=plan_row["test_plan_id"],
        name=plan_row["name"],
        env=plan_row["env"],
        created_at=plan_row["created_at"],
        results=results
    )


@api_router.get("/testplan/{test_plan_id}/from/{test_run_id}", response_model=TestPlanDetail,
                dependencies=[Depends(verify_token)])
async def get_testplan_detail_until_run(test_plan_id: str, test_run_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        plan_row = await conn.fetchrow(
            "SELECT test_plan_id, name, env, created_at FROM TestPlan WHERE test_plan_id = $1",
            test_plan_id
        )
        if not plan_row:
            raise HTTPException(status_code=404, detail="Test plan not found")
        tr_row = await conn.fetchrow(
            "SELECT test_plan_id, date_start FROM TestRun WHERE test_run_id = $1",
            test_run_id
        )
        if not tr_row:
            raise HTTPException(status_code=404, detail="Test run not found")
        if tr_row["test_plan_id"] != test_plan_id:
            raise HTTPException(status_code=404, detail="Test run not found in this test plan")
        rows = await conn.fetch("""
        SELECT 
            trt.test_id, 
            t.name,
            t.epic,
            t.story,
            t.feature,
            trt.test_run_id, 
            trt.date_start, 
            trt.date_end, 
            s.name AS status, 
            trt.stacktrace, 
            trt.message, 
            trt.number, 
            trt.steps,
            (
                SELECT STRING_AGG(f2.file_id || '::' || f2.filename, ',')
                FROM TestRunFiles f2
                WHERE f2.test_run_id = trt.test_run_id
          AND f2.file_id = ANY(
              ARRAY(
                  SELECT json_array_elements_text(
                      CASE 
                          WHEN trt.file_ids IS NULL OR trt.file_ids = '' OR trt.file_ids = '[]'
                          THEN '[]'::json
                          ELSE trt.file_ids::json
                      END
                  )
              )
          )
            ) AS files,
            trt.links,
            trt.params,
            t.params_test,
            t.tags
        FROM TestRunTests trt
        JOIN Test t ON trt.test_id = t.test_id
        JOIN Status s ON trt.status_id = s.status_id
        WHERE trt.test_run_id = $1
        GROUP BY trt.test_id, trt.number, trt.test_run_id, t.name, t.epic, t.story, t.feature,
                 trt.date_start, trt.date_end, s.name, trt.stacktrace, trt.message, trt.steps,
                 trt.links, trt.params, t.params_test, t.tags
        ORDER BY trt.test_id, trt.number
        """, test_run_id)
        results: List[TestResult] = []
        for row in rows:
            steps = json.loads(row["steps"]) if row["steps"] else []
            links = json.loads(row["links"]) if row["links"] else []
            _params_raw = json.loads(row["params"]) if row["params"] else None
            if isinstance(_params_raw, dict):
                params = [_params_raw]
            elif isinstance(_params_raw, list):
                params = _params_raw
            else:
                params = None
            params_test = json.loads(row["params_test"]) if row["params_test"] else None
            tags = []
            if row["tags"]:
                try:
                    tags = json.loads(row["tags"])
                except Exception:
                    tags = []
            files = []
            if row["files"]:
                for part in row["files"].split(','):
                    if '::' in part:
                        fid, fname = part.split('::', 1)
                        files.append({"file_id": fid, "filename": fname})
            results.append(TestResult(
                test_id=row["test_id"],
                name=row["name"],
                epic=row["epic"],
                story=row["story"],
                feature=row["feature"],
                test_run_id=row["test_run_id"],
                date_start=row["date_start"],
                date_end=row["date_end"],
                status=row["status"],
                stacktrace=row["stacktrace"],
                message=row["message"],
                number=row["number"],
                steps=steps,
                files=files,
                links=links,
                params=params,
                params_test=params_test,
                tags=tags,
            ))
    return TestPlanDetail(
        test_plan_id=plan_row["test_plan_id"],
        name=plan_row["name"],
        env=plan_row["env"],
        created_at=plan_row["created_at"],
        results=results
    )


@api_router.get("/file/{test_run_id}/{file_id}")
async def download_file(test_run_id: str, file_id: str):
    file_path = UPLOAD_DIR / test_run_id / file_id
    try:
        file_path.relative_to(UPLOAD_DIR)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid file path")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT filename FROM TestRunFiles
            WHERE test_run_id = $1 AND file_id = $2
        """, test_run_id, file_id)
    if not row:
        raise HTTPException(status_code=404, detail="File not found")
    filename = row["filename"]
    content_type, _ = mimetypes.guess_type(filename)
    media_type = content_type or "application/octet-stream"
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File content not found")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )


@api_router.post("/testplan/{test_plan_id}/testrun", response_model=TestRunResponse,
                 dependencies=[Depends(verify_token)])
async def create_testrun(test_plan_id: str, body: Optional[CreateTestRunRequest] = None):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        try:
            exists = await conn.fetchval(
                "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
                test_plan_id
            )
            if not exists:
                raise HTTPException(status_code=404, detail="Test plan not found")
            test_run_id = str(uuid.uuid4())
            date_start = datetime.now(timezone.utc).isoformat()
            params_tr_json = None
            if body and body.params_testrun is not None:
                params_tr_json = json.dumps(body.params_testrun, ensure_ascii=False)
            await conn.execute("""
                INSERT INTO TestRun (test_run_id, test_plan_id, date_start, params_testrun)
                VALUES ($1, $2, $3, $4)
            """, test_run_id, test_plan_id, date_start, params_tr_json)
        except asyncpg.IntegrityConstraintViolationError:
            raise HTTPException(status_code=400, detail="Could not create test run due to database constraint.")
        except asyncpg.PostgresError as e:
            logger.error(f"Database error while creating test run: {e}")
            raise HTTPException(status_code=500, detail="Database error occurred.")
        except Exception as e:
            logger.error(f"Unexpected error while creating test run: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail="Unexpected error.")
    try:
        parsed_params = json.loads(params_tr_json) if params_tr_json else None
    except Exception:
        parsed_params = None
    return TestRunResponse(
        test_plan_id=test_plan_id,
        test_run_id=test_run_id,
        date_start=date_start,
        params_testrun=parsed_params
    )


@api_router.post("/testrun/{test_run_id}", dependencies=[Depends(verify_token)])
async def submit_test_results(test_run_id: str, results: List[TestRunTestRequest], need_updating: bool = False):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                test_plan_row = await conn.fetchrow(
                    "SELECT test_plan_id FROM TestRun WHERE test_run_id = $1",
                    test_run_id
                )
                if not test_plan_row:
                    raise HTTPException(status_code=404, detail="Test run not found")
                test_plan_id = test_plan_row["test_plan_id"]
                for result in results:
                    test_id = result.test.test_id
                    number = int(result.number)
                    old_records = await conn.fetch("""
                        SELECT trt.test_run_id, trt.file_ids
                        FROM TestRunTests trt
                        JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
                        WHERE tr.test_plan_id = $1
                          AND trt.test_id = $2
                          AND trt.number = $3
                          AND trt.test_run_id != $4
                    """, test_plan_id, test_id, number, test_run_id)
                    for row in old_records:
                        old_test_run_id = row["test_run_id"]
                        file_ids_json = row["file_ids"]
                        if file_ids_json:
                            try:
                                file_ids = json.loads(file_ids_json)
                                for fid in file_ids:
                                    await conn.execute(
                                        "DELETE FROM TestRunFiles WHERE test_run_id = $1 AND file_id = $2",
                                        old_test_run_id, fid
                                    )
                            except json.JSONDecodeError:
                                pass
                    tms_links_json = json.dumps(result.test.tms_links) if result.test.tms_links else None
                    params_test_json = json.dumps(result.test.params_test) if getattr(result.test, 'params_test',
                                                                                      None) else None
                    tags_json = json.dumps(result.test.tags) if getattr(result.test, 'tags', None) else None
                    await conn.execute("""
                        INSERT INTO Test AS t (test_id, epic, name, story, feature, template, tms_links, params_test, tags)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (test_id) DO UPDATE SET
                            epic = COALESCE(EXCLUDED.epic, t.epic),
                            name = COALESCE(EXCLUDED.name, t.name),
                            story = COALESCE(EXCLUDED.story, t.story),
                            feature = COALESCE(EXCLUDED.feature, t.feature),
                            template = COALESCE(EXCLUDED.template, t.template),
                            tms_links = COALESCE(EXCLUDED.tms_links, t.tms_links),
                            params_test = COALESCE(EXCLUDED.params_test, t.params_test),
                            tags = COALESCE(EXCLUDED.tags, t.tags)
                    """, test_id, result.test.epic, result.test.name, result.test.story,
                                       result.test.feature, result.test.template, tms_links_json, params_test_json,
                                       tags_json)
                    status_row = await conn.fetchrow(
                        "SELECT status_id FROM Status WHERE name = $1",
                        result.status
                    )
                    if not status_row:
                        raise HTTPException(status_code=400, detail=f"Status '{result.status}' not found")
                    status_id = status_row["status_id"]
                    steps_json = json.dumps([step.dict() for step in result.steps]) if result.steps else None
                    file_ids_json = json.dumps(result.file_ids) if result.file_ids else None
                    if result.links:
                        links_data = []
                        for link in result.links:
                            link_dict = link.dict()
                            if link_dict.get('name') is None:
                                link_dict['name'] = 'Noname'
                            links_data.append(link_dict)
                        links_json = json.dumps(links_data)
                    else:
                        links_json = None
                    try:
                        if isinstance(result.params, list):
                            params_list = list(result.params)
                            if need_updating:
                                updated = False
                                for item in params_list:
                                    if isinstance(item, dict) and "need_updating" in item:
                                        item["need_updating"] = True
                                        updated = True
                                        break
                                if not updated:
                                    params_list.append({"need_updating": True})
                            params_json = json.dumps(params_list)
                        elif isinstance(result.params, dict):
                            params_dict = dict(result.params)
                            if need_updating:
                                params_dict["need_updating"] = True
                            params_json = json.dumps([params_dict])
                        elif result.params is None:
                            if need_updating:
                                params_json = json.dumps([{"need_updating": True}])
                            else:
                                params_json = None
                        else:
                            params_json = json.dumps(result.params)
                    except Exception:
                        params_json = json.dumps([{"need_updating": True}]) if need_updating else (
                            json.dumps(result.params) if result.params else None)
                    await conn.execute("""
                        INSERT INTO TestRunTests AS trt
                        (test_run_id, test_id, number, date_start, date_end, status_id, stacktrace, message, steps, file_ids, links, params) 
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (test_run_id, test_id, number) DO UPDATE SET
                            date_start = COALESCE(EXCLUDED.date_start, trt.date_start),
                            date_end = COALESCE(EXCLUDED.date_end, trt.date_end),
                            status_id = COALESCE(EXCLUDED.status_id, trt.status_id),
                            stacktrace = COALESCE(EXCLUDED.stacktrace, trt.stacktrace),
                            message = COALESCE(EXCLUDED.message, trt.message),
                            steps = COALESCE(EXCLUDED.steps, trt.steps),
                            file_ids = COALESCE(EXCLUDED.file_ids, trt.file_ids),
                            links = COALESCE(EXCLUDED.links, trt.links),
                            params = COALESCE(EXCLUDED.params, trt.params)
                    """, test_run_id, test_id, number, result.date_start, result.date_end,
                                       status_id, result.stacktrace, result.message, steps_json,
                                       file_ids_json, links_json, params_json)
            except asyncpg.exceptions.ForeignKeyViolationError as e:
                raise HTTPException(status_code=400, detail=f"Foreign key constraint violation: {str(e)}")
            except asyncpg.exceptions.UniqueViolationError as e:
                raise HTTPException(status_code=400, detail=f"Unique constraint violation: {str(e)}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    await manager.broadcast({"test_plan_id": test_plan_id, "action": "updated"})
    return {"status": "success"}


@api_router.delete("/testplan/{test_plan_id}", dependencies=[Depends(verify_token)])
async def delete_testplan(test_plan_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                exists = await conn.fetchval(
                    "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
                    test_plan_id
                )
                if not exists:
                    raise HTTPException(status_code=404, detail="Test plan not found")
                await conn.execute("""
                    DELETE FROM TestRunFiles
                    WHERE test_run_id IN (
                        SELECT test_run_id FROM TestRun WHERE test_plan_id = $1
                    )
                """, test_plan_id)
                await conn.execute("""
                    DELETE FROM TestRunTests 
                    WHERE test_run_id IN (
                        SELECT test_run_id FROM TestRun WHERE test_plan_id = $1
                    )
                """, test_plan_id)
                await conn.execute(
                    "DELETE FROM TestRun WHERE test_plan_id = $1",
                    test_plan_id
                )
                await conn.execute(
                    "DELETE FROM TestPlan WHERE test_plan_id = $1",
                    test_plan_id
                )
                await conn.execute("""
                    DELETE FROM Test 
                    WHERE deleted = TRUE 
                    AND test_id NOT IN (
                        SELECT DISTINCT test_id 
                        FROM TestRunTests
                    )
                """)
            except asyncpg.ForeignKeyViolationError as e:
                raise HTTPException(status_code=400, detail=f"Foreign key constraint violation: {str(e)}")
            except asyncpg.PostgresError as e:
                raise HTTPException(status_code=500, detail=f"Database error:  {str(e)}")
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    return {"status": "deleted"}


@api_router.delete("/testplan/{testplan_id}/delete_results", dependencies=[Depends(verify_token)])
async def delete_test_results_from_testplan(testplan_id: str, request: DeleteTestResultsRequest):
    if not request.test_ids:
        raise HTTPException(status_code=400, detail="test_ids list cannot be empty")
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                exists = await conn.fetchval(
                    "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
                    testplan_id
                )
                if not exists:
                    raise HTTPException(status_code=404, detail="Test plan not found")
                test_ids = request.test_ids
                placeholders_for_test = ','.join(f'${i + 1}' for i in range(len(test_ids)))
                query = f"SELECT test_id FROM Test WHERE test_id IN ({placeholders_for_test})"
                rows = await conn.fetch(query, *test_ids)
                existing_tests = {row["test_id"] for row in rows}
                missing_tests = set(request.test_ids) - existing_tests
                if missing_tests:
                    raise HTTPException(status_code=404, detail=f"Tests not found: {', '.join(missing_tests)}")
                rows = await conn.fetch("""
                    SELECT DISTINCT trf.test_run_id, trf.file_id, trf.filename
                    FROM TestRunFiles trf
                    JOIN TestRun tr ON trf.test_run_id = tr.test_run_id
                    JOIN TestRunTests trt ON tr.test_run_id = trt.test_run_id
                    WHERE tr.test_plan_id = $1 
                      AND trt.test_id = ANY($2)
                      AND trf.file_id = ANY(
                          ARRAY(
                              SELECT json_array_elements_text(
                                  CASE 
                                      WHEN trt.file_ids IS NULL OR trt.file_ids = '' OR trt.file_ids = '[]'
                                      THEN '[]'::json
                                      ELSE trt.file_ids::json
                                  END
                              )
                          )
                      )
                """, testplan_id, request.test_ids)
                files_to_delete = rows
                results_by_test = {}
                total_deleted_count = 0
                for test_id in request.test_ids:
                    count = await conn.fetchval("""
                        SELECT COUNT(*) 
                        FROM TestRunTests trt
                        JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
                        WHERE tr.test_plan_id = $1 AND trt.test_id = $2
                    """, testplan_id, test_id)
                    results_by_test[test_id] = count
                    total_deleted_count += count
                if files_to_delete:
                    file_ids_to_delete = [row["file_id"] for row in files_to_delete]
                    test_run_ids_to_delete = [row["test_run_id"] for row in files_to_delete]
                    await conn.execute("""
                        DELETE FROM TestRunFiles 
                        WHERE test_run_id = ANY($1) AND file_id = ANY($2)
                    """, test_run_ids_to_delete, file_ids_to_delete)
                placeholders_for_delete = ','.join(f'${i + 2}' for i in range(len(test_ids)))
                delete_query = f"""
                    DELETE FROM TestRunTests 
                    WHERE test_run_id IN (
                        SELECT test_run_id FROM TestRun WHERE test_plan_id = $1
                    ) 
                    AND test_id IN ({placeholders_for_delete})
                """
                await conn.execute(delete_query, testplan_id, *test_ids)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    await manager.broadcast({"test_plan_id": testplan_id, "action": "updated"})
    return {
        "status": "deleted",
        "testplan_id": testplan_id,
        "test_ids": request.test_ids,
        "deleted_results_by_test": results_by_test,
        "total_deleted_results_count": total_deleted_count,
        "deleted_files_count": len(files_to_delete) if files_to_delete else 0
    }


async def _cleanup_stale_testplans(exclude_ids: Optional[List[str]] = None):
    days_env = os.environ.get("APP_TEST_PLAN_MAX_AGE_DAYS")
    try:
        max_age_days = int(days_env) if days_env is not None else 180
    except Exception:
        max_age_days = 180
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    exclude_ids = exclude_ids or []
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        if exclude_ids:
            placeholders = ", ".join(f"${i}" for i in range(2, len(exclude_ids) + 2))
            query = f"""
                SELECT tp.test_plan_id
                FROM TestPlan tp
                WHERE (
                    COALESCE(
                        (SELECT MAX(tr.date_start) FROM TestRun tr WHERE tr.test_plan_id = tp.test_plan_id),
                        tp.created_at
                    ) < $1
                )
                AND tp.test_plan_id NOT IN ({placeholders})
            """
            stale_rows = await conn.fetch(query, cutoff_iso, *exclude_ids)
        else:
            query = """
                SELECT tp.test_plan_id
                FROM TestPlan tp
                WHERE (
                    COALESCE(
                        (SELECT MAX(tr.date_start) FROM TestRun tr WHERE tr.test_plan_id = tp.test_plan_id),
                        tp.created_at
                    ) < $1
                )
            """
            stale_rows = await conn.fetch(query, cutoff_iso)
        stale_plan_ids = [row["test_plan_id"] for row in stale_rows]
    if not stale_plan_ids:
        return
    for stale_id in stale_plan_ids:
        try:
            await delete_testplan(stale_id)
        except HTTPException as e:
            if e.status_code != 404:
                logger.error(f"Failed to delete stale test plan {stale_id}: {e.detail}")
        except Exception as e:
            logger.error(f"Failed to delete stale test plan {stale_id}: {e}")


async def _cleanup_orphaned_files():
    try:
        logger.info("Starting orphaned files cleanup...")
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            db_files = await conn.fetch("""
                SELECT test_run_id, file_id 
                FROM TestRunFiles
            """)
            existing_files = set()
            for row in db_files:
                existing_files.add((row["test_run_id"], row["file_id"]))
        deleted_count = 0
        deleted_size = 0
        if not UPLOAD_DIR.exists():
            logger.info("Upload directory does not exist, skipping file cleanup")
            return {"deleted_files": 0, "deleted_size_bytes": 0}
        for test_run_dir in UPLOAD_DIR.iterdir():
            if not test_run_dir.is_dir():
                continue
            test_run_id = test_run_dir.name
            for file_path in test_run_dir.iterdir():
                if not file_path.is_file():
                    continue
                file_id = file_path.name
                if (test_run_id, file_id) not in existing_files:
                    try:
                        file_size = file_path.stat().st_size
                        await aos.remove(file_path)
                        deleted_count += 1
                        deleted_size += file_size
                        logger.debug(f"Deleted orphaned file: {test_run_id}/{file_id}")
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned file {test_run_id}/{file_id}: {e}")
        for test_run_dir in UPLOAD_DIR.iterdir():
            if test_run_dir.is_dir():
                try:
                    if not any(test_run_dir.iterdir()):
                        await aos.rmdir(test_run_dir)
                        logger.debug(f"Deleted empty directory: {test_run_dir.name}")
                except Exception as e:
                    logger.error(f"Failed to delete empty directory {test_run_dir.name}: {e}")
        logger.info(f"Orphaned files cleanup completed: {deleted_count} files deleted, {deleted_size} bytes freed")
        return {"deleted_files": deleted_count, "deleted_size_bytes": deleted_size}
    except Exception as e:
        logger.error(f"Orphaned files cleanup failed: {e}")
        return {"deleted_files": 0, "deleted_size_bytes": 0}


async def _cleanup_old_testruns():
    try:
        logger.info("Starting old test runs cleanup...")
        limit_env = os.environ.get("APP_TEST_RUN_LIMIT")
        try:
            test_run_limit = int(limit_env) if limit_env is not None else 200
        except Exception:
            test_run_limit = 200
        logger.info(f"Test run limit per test plan: {test_run_limit}")
        pool = await get_db_connection()
        async with pool.acquire() as conn:
            testplans_with_counts = await conn.fetch("""
                SELECT 
                    tp.test_plan_id,
                    tp.name as test_plan_name,
                    COUNT(tr.test_run_id) as test_run_count
                FROM TestPlan tp
                LEFT JOIN TestRun tr ON tp.test_plan_id = tr.test_plan_id
                GROUP BY tp.test_plan_id, tp.name
                HAVING COUNT(tr.test_run_id) > $1
                ORDER BY COUNT(tr.test_run_id) DESC
            """, test_run_limit)
            total_deleted_runs = 0
            for row in testplans_with_counts:
                test_plan_id = row["test_plan_id"]
                test_plan_name = row["test_plan_name"]
                current_count = row["test_run_count"]
                excess_count = current_count - test_run_limit
                logger.info(
                    f"Test plan '{test_plan_name}' has {current_count} test runs, need to delete {excess_count}")
                old_testruns = await conn.fetch("""
                    SELECT test_run_id, date_start
                    FROM TestRun
                    WHERE test_plan_id = $1
                    ORDER BY date_start ASC
                    LIMIT $2
                """, test_plan_id, excess_count)
                if not old_testruns:
                    continue
                test_run_ids = [tr["test_run_id"] for tr in old_testruns]
                async with conn.transaction():
                    await conn.execute("""
                        DELETE FROM TestRunFiles
                        WHERE test_run_id = ANY($1)
                    """, test_run_ids)
                    await conn.execute("""
                        DELETE FROM TestRunTests
                        WHERE test_run_id = ANY($1)
                    """, test_run_ids)
                    await conn.execute("""
                        DELETE FROM TestRun
                        WHERE test_run_id = ANY($1)
                    """, test_run_ids)
                total_deleted_runs += len(old_testruns)
                logger.info(f"Deleted {len(old_testruns)} test runs from test plan '{test_plan_name}'")
        logger.info(f"Old test runs cleanup completed: {total_deleted_runs} test runs deleted")
        return {"deleted_test_runs": total_deleted_runs}
    except Exception as e:
        logger.error(f"Old test runs cleanup failed: {e}")
        return {"deleted_test_runs": 0}


async def background_cleanup_scheduler():
    while True:
        logger.info("Starting background cleanup task...")
        try:
            await _cleanup_stale_testplans()
            logger.info("Test plans cleanup completed successfully")
        except Exception as e:
            logger.error(f"Test plans cleanup failed: {e}")
        try:
            testruns_cleanup_result = await _cleanup_old_testruns()
            logger.info(f"Test runs cleanup completed: {testruns_cleanup_result}")
        except Exception as e:
            logger.error(f"Test runs cleanup failed: {e}")
        try:
            file_cleanup_result = await _cleanup_orphaned_files()
            logger.info(f"File cleanup completed: {file_cleanup_result}")
        except Exception as e:
            logger.error(f"File cleanup failed: {e}")
        logger.info("Background cleanup task completed")
        await asyncio.sleep(3600)


@api_router.get("/test", response_model=List[Test], dependencies=[Depends(verify_token)])
async def get_tests():
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT 
                t.test_id,
                t.name,
                t.epic,
                t.story,
                t.feature,
                t.template,
                t.tms_links,
                t.params_test,
                t.tags
            FROM Test t
            WHERE t.deleted = false
            ORDER BY t.test_id
        """)
    results = []
    for row in rows:
        tags = []
        if row["tags"]:
            try:
                tags = json.loads(row["tags"])
            except Exception:
                tags = []
        tms_links = json.loads(row["tms_links"]) if row["tms_links"] else None
        params_test = json.loads(row["params_test"]) if row["params_test"] else None
        results.append(Test(
            test_id=row["test_id"],
            name=row["name"],
            epic=row["epic"],
            story=row["story"],
            feature=row["feature"],
            template=row["template"],
            tms_links=tms_links,
            params_test=params_test,
            tags=tags
        ))
    return results


@api_router.get("/test/{test_id}", response_model=Test, dependencies=[Depends(verify_token)])
async def get_test(test_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT 
                t.test_id,
                t.name,
                t.epic,
                t.story,
                t.feature,
                t.template,
                t.tms_links,
                t.params_test,
                t.tags
            FROM Test t
            WHERE t.test_id = $1
        """, test_id)
    if not row:
        raise HTTPException(status_code=404, detail="Test not found")
    tags = []
    if row["tags"]:
        try:
            tags = json.loads(row["tags"])
        except Exception:
            tags = []
    tms_links = json.loads(row["tms_links"]) if row["tms_links"] else None
    params_test = json.loads(row["params_test"]) if row["params_test"] else None
    return Test(
        test_id=row["test_id"],
        name=row["name"],
        epic=row["epic"],
        story=row["story"],
        feature=row["feature"],
        template=row["template"],
        tms_links=tms_links,
        params_test=params_test,
        tags=tags
    )


@api_router.get("/test/{test_id}/history", response_model=TestHistoryResponse, dependencies=[Depends(verify_token)])
async def get_test_history(
        test_id: str,
        page: int = Query(1, ge=1),
        size: int = Query(50, ge=1, le=100)
):
    offset = (page - 1) * size
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM Test WHERE test_id = $1", test_id)
        if not exists:
            raise HTTPException(status_code=404, detail="Test not found")
        total = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM TestRunTests trt
            JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
            JOIN Status s ON trt.status_id = s.status_id
            WHERE trt.test_id = $1 
              AND s.name NOT IN ('waiting', 'started')
        """, test_id)
        rows = await conn.fetch("""
            SELECT 
                trt.date_start,
                trt.date_end,
                s.name AS status,
                trt.message,
                trt.number,
                tr.test_plan_id,
                tp.name AS test_plan_name,
                tp.env AS test_plan_env,
                trt.params
            FROM TestRunTests trt
            JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
            JOIN TestPlan tp ON tr.test_plan_id = tp.test_plan_id
            JOIN Status s ON trt.status_id = s.status_id
            WHERE trt.test_id = $1 
              AND s.name NOT IN ('waiting', 'started')
            ORDER BY tr.date_start DESC, trt.number
            LIMIT $2 OFFSET $3
        """, test_id, size, offset)
    history = []
    for row in rows:
        try:
            _params_raw = json.loads(row["params"]) if row["params"] else None
            if isinstance(_params_raw, dict):
                params = [_params_raw]
            elif isinstance(_params_raw, list):
                params = _params_raw
            else:
                params = None
        except Exception:
            params = None
        history.append(TestHistoryItem(
            date_start=row["date_start"],
            date_end=row["date_end"],
            status=row["status"],
            message=row["message"],
            number=row["number"],
            test_plan_id=row["test_plan_id"],
            test_plan_name=row["test_plan_name"],
            test_plan_env=row["test_plan_env"],
            params=params
        ))
    return TestHistoryResponse(
        test_id=test_id,
        history=history,
        total=total
    )


@api_router.get("/testplan/{testplan_id}/{test_id}/history",
                response_model=TestHistoryResponse,
                dependencies=[Depends(verify_token)])
async def get_test_history_testplan(
        testplan_id: str,
        test_id: str,
        page: int = Query(1, ge=1),
        size: int = Query(50, ge=1, le=100)
):
    offset = (page - 1) * size
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        plan_exists = await conn.fetchval(
            "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
            testplan_id
        )
        if not plan_exists:
            raise HTTPException(status_code=404, detail="Test plan not found")
        test_exists = await conn.fetchval(
            "SELECT 1 FROM Test WHERE test_id = $1",
            test_id
        )
        if not test_exists:
            raise HTTPException(status_code=404, detail="Test not found")
        total = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM TestRunTests trt
            JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
            JOIN Status s ON trt.status_id = s.status_id
            WHERE trt.test_id = $1 
              AND tr.test_plan_id = $2
              AND s.name NOT IN ('waiting', 'started')
        """, test_id, testplan_id)
        rows = await conn.fetch("""
            SELECT 
                trt.date_start,
                trt.date_end,
                s.name AS status,
                trt.message,
                trt.number,
                tr.test_plan_id,
                tp.name AS test_plan_name,
                tp.env AS test_plan_env,
                trt.params
            FROM TestRunTests trt
            JOIN TestRun tr ON trt.test_run_id = tr.test_run_id
            JOIN TestPlan tp ON tr.test_plan_id = tp.test_plan_id
            JOIN Status s ON trt.status_id = s.status_id
            WHERE trt.test_id = $1 
              AND tr.test_plan_id = $2
              AND s.name NOT IN ('waiting', 'started')
            ORDER BY tr.date_start DESC, trt.number
            LIMIT $3 OFFSET $4
        """, test_id, testplan_id, size, offset)
    history = []
    for row in rows:
        try:
            _params_raw = json.loads(row["params"]) if row["params"] else None
            if isinstance(_params_raw, dict):
                params = [_params_raw]
            elif isinstance(_params_raw, list):
                params = _params_raw
            else:
                params = None
        except Exception:
            params = None
        history.append(TestHistoryItem(
            date_start=row["date_start"],
            date_end=row["date_end"],
            status=row["status"],
            message=row["message"],
            number=row["number"],
            test_plan_id=row["test_plan_id"],
            test_plan_name=row["test_plan_name"],
            test_plan_env=row["test_plan_env"],
            params=params
        ))
    return TestHistoryResponse(
        test_id=test_id,
        test_plan_id=testplan_id,
        history=history,
        total=total
    )


@api_router.get("/testplan/{testplan_id}/history",
                response_model=List[TestRunHistoryItem],
                dependencies=[Depends(verify_token)])
async def get_testplan_history(
        testplan_id: str,
        page: int = Query(1, ge=1),
        size: int = Query(200, ge=1, le=200)
):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
            testplan_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Test plan not found")
        selected_runs = await conn.fetch("""
            SELECT test_run_id, date_start
            FROM TestRun
            WHERE test_plan_id = $1
            ORDER BY date_start DESC
        """, testplan_id)
        if not selected_runs:
            return []
        run_ids = [row["test_run_id"] for row in selected_runs]
        placeholders = ', '.join(f'${i}' for i in range(2, len(run_ids) + 2))
        all_rows = await conn.fetch(f"""
            SELECT 
                tr.test_run_id, tr.date_start,
                trt.test_id, trt.number, trt.status_id, s.name AS status_name
            FROM TestRun tr
            JOIN TestRunTests trt ON tr.test_run_id = trt.test_run_id
            JOIN Status s ON trt.status_id = s.status_id
            WHERE tr.test_plan_id = $1 AND tr.test_run_id IN ({placeholders})
            ORDER BY tr.date_start ASC
        """, testplan_id, *run_ids)
        status_rows = await conn.fetch("SELECT name FROM Status ORDER BY name")
        all_status_names = [row["name"] for row in status_rows]
    from collections import defaultdict
    runs = {}
    for row in all_rows:
        tr_id = row["test_run_id"]
        if tr_id not in runs:
            runs[tr_id] = {
                "test_run_id": tr_id,
                "date_start": row["date_start"],
                "results": []
            }
        runs[tr_id]["results"].append({
            "test_id": row["test_id"],
            "number": row["number"],
            "status_name": row["status_name"]
        })
    sorted_runs = sorted(runs.values(), key=lambda x: x["date_start"])
    latest_status = {}
    result = []
    for run in sorted_runs:
        for res in run["results"]:
            key = (res["test_id"], res["number"])
            latest_status[key] = res["status_name"]
        counts = defaultdict(int)
        for st in latest_status.values():
            counts[st] += 1
        statuses = [
            StatusCount(status=st, count=counts.get(st, 0))
            for st in all_status_names
            if st not in ['started', 'waiting']
        ]
        result.append(TestRunHistoryItem(
            test_run_id=run["test_run_id"],
            date_start=run["date_start"],
            statuses=statuses
        ))
    result_desc = result[::-1]
    start_index = (page - 1) * size
    end_index = start_index + size
    return result_desc[start_index:end_index]


@api_router.get("/testplan/{testplan_id}/testruns",
                response_model=List[TestRunHistoryItem],
                dependencies=[Depends(verify_token)])
async def get_testplan_testruns(
        testplan_id: str,
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=200)
):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
            testplan_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Test plan not found")
        selected_runs = await conn.fetch("""
            SELECT test_run_id, date_start, params_testrun
            FROM TestRun
            WHERE test_plan_id = $1
            ORDER BY date_start DESC
        """, testplan_id)
        if not selected_runs:
            return []
        run_ids = [row["test_run_id"] for row in selected_runs]
        status_rows = await conn.fetch("SELECT name FROM Status ORDER BY name")
        all_status_names = [row["name"] for row in status_rows]
        placeholders = ', '.join(f'${i}' for i in range(1, len(run_ids) + 1))
        counts_rows = await conn.fetch(f"""
            SELECT 
                trt.test_run_id,
                s.name AS status_name,
                COUNT(*) AS cnt
            FROM TestRunTests trt
            JOIN Status s ON trt.status_id = s.status_id
            WHERE trt.test_run_id IN ({placeholders})
            GROUP BY trt.test_run_id, s.name
        """, *run_ids)
    from collections import defaultdict
    run_to_counts = defaultdict(dict)
    for row in counts_rows:
        run_to_counts[row["test_run_id"]][row["status_name"]] = row["cnt"]
    result_desc = []
    for row in selected_runs:
        tr_id = row["test_run_id"]
        try:
            params_testrun = json.loads(row["params_testrun"]) if row["params_testrun"] else None
        except Exception:
            params_testrun = None
        counts_map = run_to_counts.get(tr_id, {})
        statuses = [
            StatusCount(status=st, count=counts_map.get(st, 0))
            for st in all_status_names
            if st not in ['started', 'waiting']
        ]
        result_desc.append(TestRunHistoryItem(
            test_run_id=tr_id,
            date_start=row["date_start"],
            statuses=statuses,
            params_testrun=params_testrun
        ))
    start_index = (page - 1) * size
    end_index = start_index + size
    return result_desc[start_index:end_index]


@api_router.get("/testplan/{test_plan_id}/metrics", response_model=TestPlanMetrics,
                dependencies=[Depends(verify_token)])
async def get_testplan_metrics(test_plan_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
            test_plan_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Test plan not found")
        runs = await conn.fetch(
            """
            SELECT tr.test_run_id, tr.date_start
            FROM TestRun tr
            WHERE tr.test_plan_id = $1
            ORDER BY tr.date_start ASC
            """,
            test_plan_id
        )
        if not runs:
            return TestPlanMetrics(
                test_plan_id=test_plan_id,
                total_test_runs_duration_sec=0,
                test_plan_duration_sec=0
            )
        total_seconds = 0
        earliest_plan_start: Optional[datetime] = None
        latest_plan_end: Optional[datetime] = None
        for row in runs:
            tr_id = row["test_run_id"]
            tr_start_iso = row["date_start"]
            try:
                tr_start_dt = datetime.fromisoformat(tr_start_iso.replace('Z', '+00:00')) if tr_start_iso else None
            except Exception:
                tr_start_dt = None
            if earliest_plan_start is None and tr_start_dt is not None:
                earliest_plan_start = tr_start_dt
            max_end_iso = await conn.fetchval(
                """
                SELECT MAX(date_end)
                FROM TestRunTests
                WHERE test_run_id = $1 AND date_end IS NOT NULL
                """,
                tr_id
            )
            tr_latest_end_dt: Optional[datetime] = None
            if max_end_iso:
                try:
                    tr_latest_end_dt = datetime.fromisoformat(max_end_iso.replace('Z', '+00:00'))
                except Exception:
                    tr_latest_end_dt = None
            if tr_latest_end_dt and (latest_plan_end is None or tr_latest_end_dt > latest_plan_end):
                latest_plan_end = tr_latest_end_dt
            if tr_start_dt and tr_latest_end_dt and tr_latest_end_dt > tr_start_dt:
                total_seconds += int((tr_latest_end_dt - tr_start_dt).total_seconds())
        if earliest_plan_start and latest_plan_end and latest_plan_end > earliest_plan_start:
            plan_seconds = int((latest_plan_end - earliest_plan_start).total_seconds())
        else:
            plan_seconds = 0
        return TestPlanMetrics(
            test_plan_id=test_plan_id,
            total_test_runs_duration_sec=total_seconds,
            test_plan_duration_sec=plan_seconds
        )
    return None


@api_router.get("/metrics/{test_plan_id}")
async def get_prometheus_metrics(test_plan_id: str):
    pool = await get_db_connection()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM TestPlan WHERE test_plan_id = $1",
            test_plan_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Test plan not found")
        rows = await conn.fetch("""
        WITH LatestTestRunTests AS (
            SELECT 
                trt.test_id,
                trt.number,
                trt.test_run_id,
                tr.date_start,
                ROW_NUMBER() OVER (
                    PARTITION BY trt.test_id, trt.number 
                    ORDER BY tr.date_start DESC
                ) AS rn
            FROM TestRun tr
            JOIN TestRunTests trt ON tr.test_run_id = trt.test_run_id
            WHERE tr.test_plan_id = $1
        )
        SELECT 
            trt.test_id, 
            t.name,
            trt.date_start, 
            trt.date_end, 
            s.name AS status
        FROM TestRunTests trt
        JOIN LatestTestRunTests l 
            ON trt.test_run_id = l.test_run_id
            AND trt.test_id = l.test_id
            AND trt.number = l.number
        JOIN Test t ON trt.test_id = t.test_id
        JOIN Status s ON trt.status_id = s.status_id
        WHERE l.rn = 1
        ORDER BY trt.test_id, trt.number
        """, test_plan_id)
        metrics = []
        for row in rows:
            metrics.append(PrometheusTestMetric(
                test_id=row["test_id"],
                test_name=row["name"],
                date_start=row["date_start"],
                date_end=row["date_end"],
                status=row["status"],
                status_code=get_status_code(row["status"])
            ))
        prometheus_output = format_prometheus_metrics(metrics)
        return Response(
            content=prometheus_output,
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )
    return None


@api_router.get("/testplan/{test_plan_id}/allure_results")
async def export_testplan_allure(test_plan_id: str):
    test_plan_detail = await get_testplan_detail(test_plan_id)
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for result in test_plan_detail.results:
            allure_result = _convert_to_allure_format(result, test_plan_detail)
            unique_id = f"{result.test_id}-{result.number}"
            filename_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, unique_id))
            filename = f"{filename_uuid}-result.json"
            zip_file.writestr(filename, json.dumps(allure_result, ensure_ascii=False, indent=2))
    zip_buffer.seek(0)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"allure-results_{test_plan_detail.test_plan_id}_{timestamp}.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/x-zip-compressed",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


def _convert_to_allure_format(result: TestResult, test_plan: TestPlanDetail) -> dict:
    start_timestamp = _convert_iso_to_timestamp(result.date_start) if result.date_start else None
    stop_timestamp = _convert_iso_to_timestamp(result.date_end) if result.date_end else None
    status_details = {
        "known": False,
        "muted": False,
        "flaky": False
    }
    if result.message:
        status_details["message"] = result.message
    if result.stacktrace:
        status_details["trace"] = result.stacktrace
    allure_steps = [_convert_step_to_allure(step) for step in result.steps]
    attachments = []
    if result.files:
        for file_ref in result.files:
            attachments.append({
                "name": file_ref.filename,
                "source": file_ref.file_id,
                "type": "text/plain"
            })
    parameters = []
    if result.params:
        for p in result.params:
            for k, v in p.items():
                parameters.append({"name": str(k), "value": str(v)})
    labels = [
        {"name": "tag", "value": "testwatch"},
        {"name": "suite", "value": test_plan.name},
        {"name": "host", "value": "testwatch"},
        {"name": "thread", "value": f"testwatch-{test_plan.test_plan_id}"},
        {"name": "framework", "value": "testwatch"},
        {"name": "language", "value": "python"},
    ]
    if result.epic:
        labels.append({"name": "epic", "value": result.epic})
    if result.feature:
        labels.append({"name": "feature", "value": result.feature})
    if result.story:
        labels.append({"name": "story", "value": result.story})
    if result.name:
        labels.extend([
            {"name": "testClass", "value": result.name},
            {"name": "testMethod", "value": result.name},
            {"name": "package", "value": result.name}
        ])
    links = []
    if result.links:
        for link in result.links:
            link_dict = {
                "name": link.name,
                "url": link.url
            }
            if link.type:
                link_dict["type"] = link.type
            links.append(link_dict)
    unique_id = f"{result.test_id}-{result.number}"
    filename_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, unique_id))
    allure_result = {
        "name": result.name or "Unnamed test",
        "status": result.status,
        "statusDetails": status_details,
        "stage": "finished",
        "description": "",
        "steps": allure_steps,
        "attachments": attachments,
        "parameters": parameters,
        "uuid": filename_uuid,
        "historyId": result.test_id,
        "labels": labels,
        "fullName": result.test_id,
        "links": links
    }
    if start_timestamp:
        allure_result["start"] = start_timestamp
    if stop_timestamp:
        allure_result["stop"] = stop_timestamp
    return allure_result


def _convert_step_to_allure(step: TestStep) -> dict:
    allure_step = {
        "name": step.name,
        "status": step.status,
        "stage": "finished"
    }
    if step.steps:
        allure_step["steps"] = [_convert_step_to_allure(substep) for substep in step.steps]
    else:
        allure_step["steps"] = []
    return allure_step


def _convert_iso_to_timestamp(iso_string: str) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return None


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            try:
                data = await websocket.receive_text()
            except WebSocketDisconnect:
                break
            except Exception:
                break
            try:
                payload = json.loads(data)
                if isinstance(payload, dict) and payload.get("type") == "pong":
                    manager.last_pong_ms[websocket] = int(datetime.now(timezone.utc).timestamp() * 1000)
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    finally:
        manager.disconnect(websocket)


app.include_router(api_router)
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    await init_database()
    logger.info("Database initialized with test data")
    asyncio.create_task(background_cleanup_scheduler())
    logger.info("Background cleanup scheduler started")
    try:
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()

        def _ignore_win_conn_reset(_loop, context):
            exc = context.get("exception")
            msg = context.get("message", "")
            if isinstance(exc, ConnectionResetError):
                return
            if "ConnectionResetError" in msg or "WinError 10054" in msg:
                return
            if previous_handler is not None:
                previous_handler(_loop, context)
            else:
                _loop.default_exception_handler(context)

        loop.set_exception_handler(_ignore_win_conn_reset)
    except Exception:
        pass


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="error", access_log=False)
