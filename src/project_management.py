"""
BlackRoad Project Management — production implementation.
Topological sort critical path, burndown, Gantt CSV, deadline alerts.
"""
from __future__ import annotations

import csv
import io
import sqlite3
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import uuid


# ─────────────────────────── data models ────────────────────────────────────

@dataclass
class Project:
    name: str
    owner: str
    deadline: date
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "active"           # active | on_hold | completed | cancelled
    description: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)

    def is_overdue(self) -> bool:
        return self.status not in ("completed", "cancelled") and date.today() > self.deadline

    def days_remaining(self) -> int:
        return (self.deadline - date.today()).days


@dataclass
class Task:
    project_id: str
    title: str
    assignee: str
    priority: int                    # 1=critical … 4=low
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "todo"             # todo | in_progress | review | done | blocked
    due_date: Optional[date] = None
    story_points: int = 1
    dependencies: List[str] = field(default_factory=list)   # list of task IDs
    completed_at: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    def is_done(self) -> bool:
        return self.status == "done"


# ──────────────────────────── database layer ────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS projects (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active',
    owner       TEXT NOT NULL,
    deadline    TEXT NOT NULL,
    description TEXT DEFAULT '',
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id           TEXT PRIMARY KEY,
    project_id   TEXT NOT NULL REFERENCES projects(id),
    title        TEXT NOT NULL,
    assignee     TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 3,
    status       TEXT NOT NULL DEFAULT 'todo',
    due_date     TEXT,
    story_points INTEGER NOT NULL DEFAULT 1,
    completed_at TEXT,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dependencies (
    task_id      TEXT NOT NULL REFERENCES tasks(id),
    depends_on   TEXT NOT NULL REFERENCES tasks(id),
    PRIMARY KEY (task_id, depends_on)
);

CREATE INDEX IF NOT EXISTS idx_tasks_project  ON tasks(project_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status   ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_deps_task      ON dependencies(task_id);
"""


class ProjectManager:
    """
    Full project management engine backed by SQLite.
    Provides critical path (topological sort), burndown chart data,
    Gantt CSV export, and proactive deadline alerting.
    """

    def __init__(self, db_path: str = ":memory:"):
        self.conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(DDL)
        self.conn.commit()

    # ── project CRUD ────────────────────────────────────────────────────────

    def create_project(self, name: str, owner: str, deadline: date,
                       description: str = "") -> Project:
        p = Project(name=name, owner=owner, deadline=deadline,
                    description=description)
        self.conn.execute(
            "INSERT INTO projects VALUES (?,?,?,?,?,?,?)",
            (p.id, p.name, p.status, p.owner, p.deadline.isoformat(),
             p.description, p.created_at.isoformat()),
        )
        self.conn.commit()
        return p

    def get_project(self, project_id: str) -> Optional[Project]:
        row = self.conn.execute(
            "SELECT * FROM projects WHERE id=?", (project_id,)
        ).fetchone()
        if not row:
            return None
        return Project(
            id=row["id"], name=row["name"], status=row["status"],
            owner=row["owner"],
            deadline=date.fromisoformat(row["deadline"]),
            description=row["description"] or "",
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    def list_projects(self, status: Optional[str] = None) -> List[Project]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM projects WHERE status=? ORDER BY deadline", (status,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM projects ORDER BY deadline"
            ).fetchall()
        return [
            Project(
                id=r["id"], name=r["name"], status=r["status"],
                owner=r["owner"], deadline=date.fromisoformat(r["deadline"]),
                description=r["description"] or "",
                created_at=datetime.fromisoformat(r["created_at"]),
            )
            for r in rows
        ]

    def update_project_status(self, project_id: str, status: str) -> bool:
        cur = self.conn.execute(
            "UPDATE projects SET status=? WHERE id=?", (status, project_id)
        )
        self.conn.commit()
        return cur.rowcount > 0

    # ── task CRUD ────────────────────────────────────────────────────────────

    def add_task(self, task: Task) -> Task:
        self.conn.execute(
            "INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?)",
            (task.id, task.project_id, task.title, task.assignee,
             task.priority, task.status,
             task.due_date.isoformat() if task.due_date else None,
             task.story_points,
             task.completed_at.isoformat() if task.completed_at else None,
             task.created_at.isoformat()),
        )
        for dep in task.dependencies:
            self.conn.execute(
                "INSERT OR IGNORE INTO dependencies VALUES (?,?)",
                (task.id, dep),
            )
        self.conn.commit()
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        row = self.conn.execute(
            "SELECT * FROM tasks WHERE id=?", (task_id,)
        ).fetchone()
        if not row:
            return None
        deps = [
            r["depends_on"]
            for r in self.conn.execute(
                "SELECT depends_on FROM dependencies WHERE task_id=?", (task_id,)
            ).fetchall()
        ]
        return self._row_to_task(row, deps)

    def _row_to_task(self, row: sqlite3.Row, deps: List[str]) -> Task:
        return Task(
            id=row["id"], project_id=row["project_id"], title=row["title"],
            assignee=row["assignee"], priority=row["priority"],
            status=row["status"],
            due_date=date.fromisoformat(row["due_date"]) if row["due_date"] else None,
            story_points=row["story_points"],
            dependencies=deps,
            completed_at=datetime.fromisoformat(row["completed_at"])
                         if row["completed_at"] else None,
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    def get_project_tasks(self, project_id: str) -> List[Task]:
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE project_id=? ORDER BY priority, due_date",
            (project_id,),
        ).fetchall()
        result = []
        for row in rows:
            deps = [
                r["depends_on"]
                for r in self.conn.execute(
                    "SELECT depends_on FROM dependencies WHERE task_id=?",
                    (row["id"],),
                ).fetchall()
            ]
            result.append(self._row_to_task(row, deps))
        return result

    def update_task_status(self, task_id: str, status: str) -> bool:
        completed_at = (
            datetime.utcnow().isoformat() if status == "done" else None
        )
        cur = self.conn.execute(
            "UPDATE tasks SET status=?, completed_at=? WHERE id=?",
            (status, completed_at, task_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def add_dependency(self, task_id: str, depends_on: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO dependencies VALUES (?,?)",
            (task_id, depends_on),
        )
        self.conn.commit()

    # ── critical path ────────────────────────────────────────────────────────

    def get_critical_path(self, project_id: str) -> List[Task]:
        """
        Return tasks on the critical path using Kahn's topological sort
        weighted by story points.  The longest-duration chain (in story
        points) is returned in execution order.
        """
        tasks = self.get_project_tasks(project_id)
        if not tasks:
            return []

        by_id: Dict[str, Task] = {t.id: t for t in tasks}

        # Build adjacency list and in-degree map
        in_deg: Dict[str, int] = {t.id: 0 for t in tasks}
        children: Dict[str, List[str]] = defaultdict(list)
        for task in tasks:
            for dep in task.dependencies:
                if dep in by_id:          # guard against orphan deps
                    in_deg[task.id] += 1
                    children[dep].append(task.id)

        # Kahn's BFS + longest-path tracking
        earliest: Dict[str, int] = {t.id: 0 for t in tasks}
        pred: Dict[str, Optional[str]] = {t.id: None for t in tasks}
        queue: deque = deque(tid for tid, deg in in_deg.items() if deg == 0)
        topo_order: List[str] = []

        while queue:
            tid = queue.popleft()
            topo_order.append(tid)
            task = by_id[tid]
            finish = earliest[tid] + task.story_points
            for child_id in children[tid]:
                if finish > earliest[child_id]:
                    earliest[child_id] = finish
                    pred[child_id] = tid
                in_deg[child_id] -= 1
                if in_deg[child_id] == 0:
                    queue.append(child_id)

        if len(topo_order) != len(tasks):
            # Cycle detected — return priority-sorted tasks as fallback
            return sorted(tasks, key=lambda t: t.priority)

        # Trace back from the task with the maximum earliest finish
        end_id = max(topo_order, key=lambda tid: earliest[tid] + by_id[tid].story_points)
        path: List[str] = []
        cur: Optional[str] = end_id
        while cur is not None:
            path.append(cur)
            cur = pred[cur]
        path.reverse()
        return [by_id[tid] for tid in path]

    # ── burndown ─────────────────────────────────────────────────────────────

    def calculate_burndown(
        self, project_id: str, sprint_days: int = 14
    ) -> List[Dict[str, Any]]:
        """
        Return ideal vs actual remaining story-point burndown for each day
        of the sprint.  Days without completions carry the last actual value
        forward.
        """
        tasks = self.get_project_tasks(project_id)
        total_points = sum(t.story_points for t in tasks)
        if total_points == 0:
            return []

        start = date.today() - timedelta(days=sprint_days)
        ideal_per_day = total_points / sprint_days

        # Map date → story points completed on that day
        completed_by_day: Dict[date, int] = defaultdict(int)
        for task in tasks:
            if task.is_done() and task.completed_at:
                d = task.completed_at.date()
                if d >= start:
                    completed_by_day[d] += task.story_points

        chart: List[Dict[str, Any]] = []
        remaining = total_points
        for i in range(sprint_days + 1):
            day = start + timedelta(days=i)
            remaining -= completed_by_day.get(day, 0)
            chart.append({
                "day": i,
                "date": day.isoformat(),
                "ideal": round(total_points - ideal_per_day * i, 2),
                "actual": remaining,
            })
        return chart

    # ── Gantt CSV ────────────────────────────────────────────────────────────

    def export_gantt_csv(self, project_id: str) -> str:
        """Return a CSV string suitable for import into spreadsheet tools."""
        project = self.get_project(project_id)
        if not project:
            raise ValueError(f"Project {project_id!r} not found")
        tasks = self.get_project_tasks(project_id)

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "Task ID", "Title", "Assignee", "Priority", "Status",
            "Story Points", "Due Date", "Dependencies", "Critical Path",
        ])
        cp_ids = {t.id for t in self.get_critical_path(project_id)}
        for task in tasks:
            writer.writerow([
                task.id[:8],
                task.title,
                task.assignee,
                task.priority,
                task.status,
                task.story_points,
                task.due_date.isoformat() if task.due_date else "",
                "|".join(d[:8] for d in task.dependencies),
                "YES" if task.id in cp_ids else "no",
            ])
        return buf.getvalue()

    # ── deadline alerts ──────────────────────────────────────────────────────

    def check_deadlines(self, days_ahead: int = 7) -> List[Dict[str, Any]]:
        """
        Return projects and tasks whose deadlines fall within *days_ahead*
        days, grouped by urgency (overdue / today / upcoming).
        """
        today = date.today()
        horizon = today + timedelta(days=days_ahead)
        alerts: List[Dict[str, Any]] = []

        for project in self.list_projects(status="active"):
            delta = (project.deadline - today).days
            if delta <= days_ahead:
                urgency = "overdue" if delta < 0 else ("today" if delta == 0 else "upcoming")
                alerts.append({
                    "type": "project",
                    "id": project.id,
                    "name": project.name,
                    "owner": project.owner,
                    "deadline": project.deadline.isoformat(),
                    "days_remaining": delta,
                    "urgency": urgency,
                })

        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status NOT IN ('done','blocked') "
            "AND due_date IS NOT NULL AND due_date <= ?",
            (horizon.isoformat(),),
        ).fetchall()
        for row in rows:
            due = date.fromisoformat(row["due_date"])
            delta = (due - today).days
            urgency = "overdue" if delta < 0 else ("today" if delta == 0 else "upcoming")
            alerts.append({
                "type": "task",
                "id": row["id"],
                "title": row["title"],
                "assignee": row["assignee"],
                "due_date": row["due_date"],
                "days_remaining": delta,
                "urgency": urgency,
            })

        alerts.sort(key=lambda a: a["days_remaining"])
        return alerts

    # ── velocity / statistics ────────────────────────────────────────────────

    def get_project_stats(self, project_id: str) -> Dict[str, Any]:
        tasks = self.get_project_tasks(project_id)
        total = len(tasks)
        done = sum(1 for t in tasks if t.is_done())
        total_sp = sum(t.story_points for t in tasks)
        done_sp = sum(t.story_points for t in tasks if t.is_done())
        by_assignee: Dict[str, int] = defaultdict(int)
        for t in tasks:
            by_assignee[t.assignee] += t.story_points
        return {
            "total_tasks": total,
            "done_tasks": done,
            "pct_done": round(done / total * 100, 1) if total else 0,
            "total_story_points": total_sp,
            "done_story_points": done_sp,
            "by_assignee": dict(by_assignee),
        }

    def close(self) -> None:
        self.conn.close()
