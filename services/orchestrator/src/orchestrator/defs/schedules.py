from datetime import timedelta

import dagster as dg

from orchestrator.defs.jobs import compaction_job


@dg.schedule(job=compaction_job, cron_schedule="10 0 * * *")
def compaction_schedule(context: dg.ScheduleEvaluationContext):
    yesterday = (context.scheduled_execution_time - timedelta(days=1)).strftime(
        "%Y%m%d"
    )
    return dg.RunRequest(partition_key=yesterday)
