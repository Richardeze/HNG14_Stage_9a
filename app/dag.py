import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models import Job, JobStatus

logger = logging.getLogger(__name__)


class DAGValidator:
    """
    Validates and resolves job dependencies.
    
    A DAG (Directed Acyclic Graph) means jobs can depend on other jobs,
    but there can be no circular dependencies (A depends on B depends on A
    is not allowed — that would be a cycle, never resolves).
    
    Example:
        Generate Report → Upload File → Send Email
        
    Send Email won't run until Upload File completes.
    Upload File won't run until Generate Report completes.
    """

    async def validate(self, session: AsyncSession, job_id: str, dependencies: list[str]) -> tuple[bool, str]:
        """
        Check that:
        1. All dependency job IDs actually exist in the database
        2. There are no circular dependencies
        
        Returns (True, "") if valid
        Returns (False, "error message") if invalid
        """
        if not dependencies:
            return True, ""

        result = await session.execute(
            select(Job).where(Job.id.in_(dependencies))
        )
        found_jobs = result.scalars().all()
        found_ids = {job.id for job in found_jobs}

        missing = set(dependencies) - found_ids
        if missing:
            return False, f"Dependency job IDs not found: {missing}"

        # Check for circular dependencies
        has_cycle, cycle_msg = await self._detect_cycle(session, job_id, dependencies)
        if has_cycle:
            return False, cycle_msg

        return True, ""

    async def _detect_cycle(
        self, session: AsyncSession, job_id: str, dependencies: list[str]
    ) -> tuple[bool, str]:
        """
        DFS-based cycle detection.
        
        We walk the dependency tree. If we ever encounter the original
        job_id while walking, we have a cycle.
        
        Example cycle:
            Job A depends on Job B
            Job B depends on Job A  ← cycle detected here
        """
        visited = set()

        async def dfs(current_id: str) -> bool:
            if current_id == job_id:
                return True  # cycle found
            if current_id in visited:
                return False
            visited.add(current_id)

            result = await session.execute(
                select(Job).where(Job.id == current_id)
            )
            current_job = result.scalar_one_or_none()
            if not current_job or not current_job.dependencies:
                return False

            for dep_id in current_job.dependencies:
                if await dfs(dep_id):
                    return True
            return False

        for dep_id in dependencies:
            if await dfs(dep_id):
                return True, f"Circular dependency detected involving job {dep_id}"

        return False, ""

    async def get_ready_jobs(self, session: AsyncSession) -> list[Job]:
        """
        Returns all pending jobs whose dependencies are all completed.
        These are the jobs that are ready to run right now.
        """
        result = await session.execute(
            select(Job).where(Job.status == JobStatus.pending)
        )
        pending_jobs = result.scalars().all()

        ready = []
        for job in pending_jobs:
            if not job.dependencies:
                ready.append(job)
                continue

            # Check if all dependencies are completed
            dep_result = await session.execute(
                select(Job).where(Job.id.in_(job.dependencies))
            )
            dep_jobs = dep_result.scalars().all()

            all_done = all(d.status == JobStatus.completed for d in dep_jobs)
            if all_done:
                ready.append(job)

        logger.info("DAG check: %d jobs ready to run", len(ready))
        return ready

    async def get_dependency_chain(self, session: AsyncSession, job_id: str) -> dict:
        """
        Returns the full dependency tree for a job.
        Useful for the UI to show what a job is waiting on.
        
        Example return:
        {
            "job_id": "abc",
            "status": "pending",
            "depends_on": [
                {"job_id": "xyz", "status": "completed", "depends_on": []}
            ]
        }
        """
        result = await session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()

        if not job:
            return {}

        chain = {
            "job_id": job.id,
            "type": job.type,
            "status": job.status,
            "depends_on": [],
        }

        for dep_id in (job.dependencies or []):
            dep_chain = await self.get_dependency_chain(session, dep_id)
            chain["depends_on"].append(dep_chain)

        return chain


# Global instance
dag_validator = DAGValidator()