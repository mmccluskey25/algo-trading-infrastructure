from datetime import timedelta

import dagster as dg

from orchestrator.defs.jobs import compaction_job
from orchestrator.defs.partitions import instrument_partition


@dg.schedule(job=compaction_job, cron_schedule="10 0 * * *")
def compaction_daily_schedule(context: dg.ScheduleEvaluationContext):
    yesterday = (context.scheduled_execution_time - timedelta(days=1)).strftime(
        "%Y%m%d"
    )

    run_requests = []
    for instrument in instrument_partition.get_partition_keys():
        run_requests.append(
            dg.RunRequest(
                partition_key=dg.MultiPartitionKey(
                    {"date": yesterday, "instrument": instrument}
                ),
                run_key=f"{yesterday}_{instrument}",
            )
        )
    return run_requests
