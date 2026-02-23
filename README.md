# blackroad-project-management

> Production Python project management engine — part of [BlackRoad Foundation](https://github.com/BlackRoad-Foundation).

## Features

- **Project & Task CRUD** — SQLite-backed dataclasses with full lifecycle management
- **Critical Path** — Kahn's topological sort weighted by story points
- **Burndown Chart** — Ideal vs actual remaining points per sprint day
- **Gantt CSV Export** — Spreadsheet-ready with critical-path annotation
- **Deadline Alerts** — Proactive warnings with overdue/today/upcoming urgency levels
- **Project Statistics** — Per-assignee workload, completion percentages

## Quick Start

```python
from datetime import date, timedelta
from src.project_management import ProjectManager, Task

pm = ProjectManager("projects.db")

project = pm.create_project("Website Redesign", "alice@example.com",
                             date.today() + timedelta(days=60))

t1 = Task(project_id=project.id, title="Wireframes", assignee="bob",
          priority=2, story_points=5)
t2 = Task(project_id=project.id, title="Backend API", assignee="carol",
          priority=1, story_points=13, dependencies=[t1.id])
pm.add_task(t1)
pm.add_task(t2)

# Critical path
critical = pm.get_critical_path(project.id)
print([t.title for t in critical])   # ['Wireframes', 'Backend API']

# Burndown data
chart = pm.calculate_burndown(project.id, sprint_days=14)

# Gantt CSV
csv_data = pm.export_gantt_csv(project.id)

# Upcoming deadlines
alerts = pm.check_deadlines(days_ahead=7)
```

## Database Schema

```
projects       — id, name, status, owner, deadline, description, created_at
tasks          — id, project_id, title, assignee, priority, status, due_date,
                 story_points, completed_at, created_at
dependencies   — task_id, depends_on
```

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

## License

© BlackRoad OS, Inc. All rights reserved.
