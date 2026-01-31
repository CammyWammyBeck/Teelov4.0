"""
Scrape queue management with retry logic.

Instead of running scrapes directly (which can lose data on failures),
we use a queue-based approach where:
1. Tasks are added to a queue with priority
2. A worker processes tasks from the queue
3. Failed tasks are retried with exponential backoff
4. Permanently failed tasks are logged for investigation

This ensures no data is lost due to transient failures (network issues,
rate limiting, website maintenance, etc.).
"""

from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from teelo.db.models import ScrapeQueue


class ScrapeQueueManager:
    """
    Manages the scraping task queue.

    Provides methods to:
    - Add tasks to the queue
    - Get next task to process
    - Mark tasks as completed/failed
    - Handle retry logic with exponential backoff
    - Get queue statistics

    Usage:
        manager = ScrapeQueueManager(db_session)

        # Add a task
        task_id = manager.enqueue(
            task_type="tournament_results",
            params={"tournament_id": "australian-open", "year": 2024},
            priority=3,
        )

        # Process tasks
        while task := manager.get_next_task():
            manager.mark_in_progress(task.id)
            try:
                process_task(task)
                manager.mark_completed(task.id)
            except Exception as e:
                manager.mark_failed(task.id, str(e))
    """

    # Priority levels
    PRIORITY_URGENT = 1  # Current day's matches
    PRIORITY_HIGH = 3    # Current tournament
    PRIORITY_NORMAL = 5  # Recent historical
    PRIORITY_LOW = 7     # Older historical
    PRIORITY_BACKFILL = 9  # Historical backfill

    def __init__(self, db: Session):
        """
        Initialize the queue manager.

        Args:
            db: SQLAlchemy session for database operations
        """
        self.db = db

    def enqueue(
        self,
        task_type: str,
        params: dict,
        priority: int = 5,
        max_attempts: int = 3,
    ) -> int:
        """
        Add a task to the queue.

        Args:
            task_type: Type of scraping task:
                - 'tournament_results': Scrape completed matches
                - 'fixtures': Scrape upcoming matches
                - 'odds': Scrape betting odds
                - 'player_profile': Scrape player details
                - 'historical_tournament': Backfill historical data
            params: Task-specific parameters (JSON serializable)
            priority: Task priority (1=highest, 10=lowest)
            max_attempts: Maximum retry attempts before permanent failure

        Returns:
            ID of the created task

        Example:
            task_id = manager.enqueue(
                task_type="tournament_results",
                params={
                    "tournament_id": "australian-open",
                    "year": 2024,
                    "include_qualifying": True,
                },
                priority=3,
            )
        """
        # Check for duplicate task
        existing = self.db.query(ScrapeQueue).filter(
            ScrapeQueue.task_type == task_type,
            ScrapeQueue.task_params == params,
            ScrapeQueue.status.in_(["pending", "in_progress", "retry"]),
        ).first()

        if existing:
            # Task already queued, return existing ID
            return existing.id

        task = ScrapeQueue(
            task_type=task_type,
            task_params=params,
            priority=priority,
            max_attempts=max_attempts,
            status="pending",
        )

        self.db.add(task)
        self.db.commit()

        return task.id

    def enqueue_batch(
        self,
        tasks: list[dict],
        priority: int = 5,
    ) -> list[int]:
        """
        Add multiple tasks to the queue efficiently.

        Args:
            tasks: List of dicts with 'task_type' and 'params' keys
            priority: Priority for all tasks

        Returns:
            List of created task IDs
        """
        task_ids = []

        for task_data in tasks:
            task_id = self.enqueue(
                task_type=task_data["task_type"],
                params=task_data["params"],
                priority=task_data.get("priority", priority),
            )
            task_ids.append(task_id)

        return task_ids

    def get_next_task(self) -> Optional[ScrapeQueue]:
        """
        Get the highest priority task that's ready to process.

        Returns tasks in order of:
        1. Priority (lowest number first)
        2. Created time (oldest first)

        Only returns tasks that are:
        - Status 'pending' or 'retry'
        - Past their retry wait time (if retry)

        Returns:
            ScrapeQueue task or None if no tasks available
        """
        now = datetime.utcnow()

        task = (
            self.db.query(ScrapeQueue)
            .filter(
                ScrapeQueue.status.in_(["pending", "retry"]),
                or_(
                    ScrapeQueue.next_retry_at.is_(None),
                    ScrapeQueue.next_retry_at <= now,
                ),
            )
            .order_by(
                ScrapeQueue.priority.asc(),
                ScrapeQueue.created_at.asc(),
            )
            .first()
        )

        return task

    def mark_in_progress(self, task_id: int) -> None:
        """
        Mark a task as being processed.

        Should be called immediately after get_next_task() before
        starting the actual work.

        Args:
            task_id: ID of the task to mark
        """
        self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).update({
            "status": "in_progress",
            "started_at": datetime.utcnow(),
            "attempts": ScrapeQueue.attempts + 1,
        })
        self.db.commit()

    def mark_completed(self, task_id: int) -> None:
        """
        Mark a task as successfully completed.

        Args:
            task_id: ID of the completed task
        """
        self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).update({
            "status": "completed",
            "completed_at": datetime.utcnow(),
        })
        self.db.commit()

    def mark_failed(self, task_id: int, error: str) -> None:
        """
        Mark a task as failed, scheduling retry if attempts remain.

        Uses exponential backoff for retry delays:
        - Attempt 1 fails: retry in 5 minutes
        - Attempt 2 fails: retry in 10 minutes
        - Attempt 3 fails: retry in 20 minutes
        - etc.

        Args:
            task_id: ID of the failed task
            error: Error message for logging
        """
        task = self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).first()

        if not task:
            return

        if task.attempts < task.max_attempts:
            # Schedule retry with exponential backoff
            # Base delay of 5 minutes, doubling each attempt
            delay_minutes = 5 * (2 ** (task.attempts - 1))
            next_retry = datetime.utcnow() + timedelta(minutes=delay_minutes)

            self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).update({
                "status": "retry",
                "last_error": error[:1000] if error else None,  # Truncate long errors
                "next_retry_at": next_retry,
            })
        else:
            # Max attempts reached - mark as permanently failed
            self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).update({
                "status": "failed",
                "last_error": error[:1000] if error else None,
                "completed_at": datetime.utcnow(),
            })

        self.db.commit()

    def reset_task(self, task_id: int) -> None:
        """
        Reset a failed task to pending status.

        Use this to manually retry a permanently failed task after
        investigating and fixing the underlying issue.

        Args:
            task_id: ID of the task to reset
        """
        self.db.query(ScrapeQueue).filter(ScrapeQueue.id == task_id).update({
            "status": "pending",
            "attempts": 0,
            "last_error": None,
            "next_retry_at": None,
            "started_at": None,
            "completed_at": None,
        })
        self.db.commit()

    def cancel_task(self, task_id: int) -> None:
        """
        Cancel a pending task.

        Args:
            task_id: ID of the task to cancel
        """
        self.db.query(ScrapeQueue).filter(
            ScrapeQueue.id == task_id,
            ScrapeQueue.status.in_(["pending", "retry"]),
        ).delete()
        self.db.commit()

    def get_stats(self) -> dict:
        """
        Get queue statistics.

        Returns:
            Dictionary with counts by status and other metrics
        """
        from sqlalchemy import func

        stats = {}

        # Count by status
        status_counts = (
            self.db.query(
                ScrapeQueue.status,
                func.count(ScrapeQueue.id),
            )
            .group_by(ScrapeQueue.status)
            .all()
        )

        for status, count in status_counts:
            stats[f"count_{status}"] = count

        # Total
        stats["total"] = sum(c for _, c in status_counts)

        # Ready to process (pending + retry past wait time)
        now = datetime.utcnow()
        ready_count = (
            self.db.query(func.count(ScrapeQueue.id))
            .filter(
                ScrapeQueue.status.in_(["pending", "retry"]),
                or_(
                    ScrapeQueue.next_retry_at.is_(None),
                    ScrapeQueue.next_retry_at <= now,
                ),
            )
            .scalar()
        )
        stats["ready_to_process"] = ready_count

        # Average attempts for failed tasks
        avg_attempts = (
            self.db.query(func.avg(ScrapeQueue.attempts))
            .filter(ScrapeQueue.status == "failed")
            .scalar()
        )
        stats["avg_failed_attempts"] = round(float(avg_attempts or 0), 1)

        return stats

    def get_failed_tasks(self, limit: int = 50) -> list[ScrapeQueue]:
        """
        Get recently failed tasks for review.

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of failed ScrapeQueue tasks
        """
        return (
            self.db.query(ScrapeQueue)
            .filter(ScrapeQueue.status == "failed")
            .order_by(ScrapeQueue.completed_at.desc())
            .limit(limit)
            .all()
        )

    def cleanup_old_completed(self, days: int = 30) -> int:
        """
        Remove completed tasks older than specified days.

        Args:
            days: Delete tasks completed more than this many days ago

        Returns:
            Number of tasks deleted
        """
        cutoff = datetime.utcnow() - timedelta(days=days)

        deleted = (
            self.db.query(ScrapeQueue)
            .filter(
                ScrapeQueue.status == "completed",
                ScrapeQueue.completed_at < cutoff,
            )
            .delete()
        )

        self.db.commit()
        return deleted

    def pending_count(self) -> int:
        """Get count of pending tasks."""
        from sqlalchemy import func

        return (
            self.db.query(func.count(ScrapeQueue.id))
            .filter(ScrapeQueue.status.in_(["pending", "retry"]))
            .scalar() or 0
        )
