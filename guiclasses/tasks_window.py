from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from PySide6.QtCore import QTimer, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


class TasksWindow(QWidget):
    def __init__(
        self,
        task_provider: Callable[[], list[dict[str, Any]]],
        stop_task: Callable[[int], bool],
        parent=None,
    ):
        super().__init__(parent)
        self.setWindowFlags(
            Qt.WindowType.Window
            | Qt.WindowType.WindowTitleHint
            | Qt.WindowType.WindowCloseButtonHint
            | Qt.WindowType.WindowMinMaxButtonsHint
        )
        self.setWindowTitle("Running Tasks")
        self.resize(920, 360)
        self.setMinimumSize(760, 260)
        self._task_provider = task_provider
        self._stop_task = stop_task

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        self.summary_label = QLabel("No running tasks")
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setFixedHeight(24)
        self.refresh_btn.clicked.connect(self.refresh_tasks)
        header_row.addWidget(self.summary_label)
        header_row.addStretch()
        header_row.addWidget(self.refresh_btn)
        layout.addLayout(header_row)

        self.table = QTableWidget(0, 6, self)
        self.table.setHorizontalHeaderLabels(["Owner", "Task", "Worker", "Launched by", "Running", "Stop"])
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Interactive)
        layout.addWidget(self.table)

        self._refresh_timer = QTimer(self)
        self._refresh_timer.setInterval(1000)
        self._refresh_timer.timeout.connect(self.refresh_tasks)
        self.refresh_tasks()

    def showEvent(self, event):
        self._refresh_timer.start()
        self.refresh_tasks()
        super().showEvent(event)

    def hideEvent(self, event):
        self._refresh_timer.stop()
        super().hideEvent(event)

    def refresh_tasks(self) -> None:
        tasks = list(self._task_provider())
        self.table.clearContents()
        self.table.setRowCount(len(tasks) if tasks else 1)
        self.summary_label.setText(f"Running tasks: {len(tasks)}")
        now = time.time()

        if not tasks:
            placeholder = QTableWidgetItem("No running background tasks")
            placeholder.setFlags(placeholder.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.table.setItem(0, 0, placeholder)
            for column in range(1, 5):
                empty_item = QTableWidgetItem("")
                empty_item.setFlags(empty_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.table.setItem(0, column, empty_item)
            disabled_stop = QPushButton("Stop")
            disabled_stop.setEnabled(False)
            disabled_stop.setFixedHeight(24)
            self.table.setCellWidget(0, 5, disabled_stop)
            return

        for row_index, task in enumerate(tasks):
            owner_name = str(task.get("owner_name", ""))
            task_name = str(task.get("task_name", ""))
            worker_name = str(task.get("worker_name", ""))
            launched_by = str(task.get("launched_by", ""))
            started_at = float(task.get("started_at", now))
            running_seconds = max(0, int(now - started_at))
            running_for = f"{running_seconds}s"
            thread_id = int(task.get("thread_id", 0))

            for column, value in enumerate([owner_name, task_name, worker_name, launched_by, running_for]):
                item = QTableWidgetItem(value)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.table.setItem(row_index, column, item)

            stop_btn = QPushButton("Stop")
            stop_btn.setFixedHeight(24)
            stop_btn.clicked.connect(lambda _checked=False, tid=thread_id: self._on_stop_clicked(tid))
            self.table.setCellWidget(row_index, 5, stop_btn)

    def _on_stop_clicked(self, thread_id: int) -> None:
        self._stop_task(int(thread_id))
        QTimer.singleShot(150, self.refresh_tasks)