"""Tests for BlackRoad Project Management."""
import pytest
from datetime import date, timedelta, datetime
from project_management import ProjectManager, Project, Task


@pytest.fixture
def pm():
    mgr = ProjectManager(":memory:")
    yield mgr
    mgr.close()


@pytest.fixture
def sample_project(pm):
    return pm.create_project(
        "Alpha", "alice@br.io", date.today() + timedelta(days=30)
    )


# ── test 1: create and retrieve project ─────────────────────────────────────
def test_create_and_get_project(pm):
    deadline = date.today() + timedelta(days=10)
    p = pm.create_project("Beta", "bob@br.io", deadline)
    fetched = pm.get_project(p.id)
    assert fetched is not None
    assert fetched.name == "Beta"
    assert fetched.owner == "bob@br.io"
    assert fetched.deadline == deadline
    assert fetched.status == "active"


# ── test 2: add tasks and update status ─────────────────────────────────────
def test_add_tasks_and_update_status(pm, sample_project):
    t = Task(project_id=sample_project.id, title="Design DB",
             assignee="carol", priority=2, story_points=3)
    pm.add_task(t)
    pm.update_task_status(t.id, "done")
    fetched = pm.get_task(t.id)
    assert fetched is not None
    assert fetched.status == "done"
    assert fetched.completed_at is not None


# ── test 3: critical path — linear chain ────────────────────────────────────
def test_critical_path_linear(pm, sample_project):
    pid = sample_project.id
    t1 = Task(project_id=pid, title="T1", assignee="x", priority=1, story_points=2)
    t2 = Task(project_id=pid, title="T2", assignee="x", priority=1, story_points=5,
              dependencies=[t1.id])
    t3 = Task(project_id=pid, title="T3", assignee="x", priority=1, story_points=1,
              dependencies=[t2.id])
    for t in (t1, t2, t3):
        pm.add_task(t)
    cp = pm.get_critical_path(pid)
    assert len(cp) == 3
    assert cp[0].id == t1.id
    assert cp[-1].id == t3.id


# ── test 4: burndown chart shape ─────────────────────────────────────────────
def test_burndown_shape(pm, sample_project):
    pid = sample_project.id
    for i in range(3):
        t = Task(project_id=pid, title=f"T{i}", assignee="dev",
                 priority=3, story_points=2)
        pm.add_task(t)
    chart = pm.calculate_burndown(pid, sprint_days=5)
    assert len(chart) == 6            # day 0 … day 5
    assert chart[0]["ideal"] == pytest.approx(6.0, 0.01)
    assert chart[-1]["ideal"] == pytest.approx(0.0, 0.01)


# ── test 5: gantt CSV contains headers ───────────────────────────────────────
def test_gantt_csv(pm, sample_project):
    pid = sample_project.id
    t = Task(project_id=pid, title="CSV Task", assignee="dave",
             priority=2, story_points=1)
    pm.add_task(t)
    csv_output = pm.export_gantt_csv(pid)
    assert "Task ID" in csv_output
    assert "Critical Path" in csv_output
    assert "CSV Task" in csv_output


# ── test 6: deadline alerts ───────────────────────────────────────────────────
def test_check_deadlines(pm):
    pm.create_project("Urgent", "ed@br.io", date.today() + timedelta(days=2))
    pm.create_project("Far", "ed@br.io", date.today() + timedelta(days=60))
    alerts = pm.check_deadlines(days_ahead=7)
    assert any(a["name"] == "Urgent" for a in alerts)
    assert not any(a["name"] == "Far" for a in alerts)


# ── test 7: project stats ─────────────────────────────────────────────────────
def test_project_stats(pm, sample_project):
    pid = sample_project.id
    t1 = Task(project_id=pid, title="Done", assignee="a", priority=1, story_points=4)
    t2 = Task(project_id=pid, title="Todo", assignee="b", priority=2, story_points=2)
    pm.add_task(t1)
    pm.add_task(t2)
    pm.update_task_status(t1.id, "done")
    stats = pm.get_project_stats(pid)
    assert stats["total_tasks"] == 2
    assert stats["done_tasks"] == 1
    assert stats["pct_done"] == 50.0
    assert stats["done_story_points"] == 4


# ── test 8: cycle fallback returns all tasks ─────────────────────────────────
def test_critical_path_cycle_fallback(pm, sample_project):
    pid = sample_project.id
    t1 = Task(project_id=pid, title="A", assignee="x", priority=1, story_points=1)
    t2 = Task(project_id=pid, title="B", assignee="x", priority=1, story_points=1,
              dependencies=[t1.id])
    pm.add_task(t1)
    pm.add_task(t2)
    # Artificially create a cycle at DB level
    pm.conn.execute("INSERT OR IGNORE INTO dependencies VALUES (?,?)", (t1.id, t2.id))
    pm.conn.commit()
    cp = pm.get_critical_path(pid)
    # Should gracefully return sorted tasks without crashing
    assert len(cp) == 2
