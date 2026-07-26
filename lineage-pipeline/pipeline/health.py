"""健康检查(设计方案 12 章监控):cron 定时执行,退出码非 0 由值班告警接管。

检查项:全量重建时效 / failed 队列规模 / 闭包表非空 / 上次运行是否 aborted。
"""

from datetime import datetime, timedelta

from . import config


def check(conn) -> dict:
    issues = []
    with conn.cursor() as cur:
        cur.execute("""SELECT status, finished_at FROM pipeline_run
                       WHERE run_type='full' ORDER BY run_id DESC LIMIT 1""")
        last = cur.fetchone()
        if not last:
            issues.append("no_full_rebuild_ever")
        else:
            if last["status"] == "aborted":
                issues.append("last_full_rebuild_aborted")   # 影子表校验未过,血缘停更
            if last["status"] == "failed":
                issues.append("last_full_rebuild_failed")
            if (last["finished_at"] and datetime.now() - last["finished_at"]
                    > timedelta(hours=config.HEALTH_MAX_FULL_RUN_AGE_HOURS)):
                issues.append("full_rebuild_stale")

        cur.execute("""SELECT COUNT(*) AS c FROM sql_repository
                       WHERE is_active=1 AND parse_status='failed'""")
        failed = cur.fetchone()["c"]
        if failed > config.HEALTH_MAX_FAILED_SQL:
            issues.append(f"failed_queue_too_large:{failed}")

        cur.execute("SELECT COUNT(*) AS c FROM table_closure")
        if cur.fetchone()["c"] == 0:
            issues.append("closure_empty")

    return {"healthy": not issues, "issues": issues,
            "failed_sql_count": failed, "checked_at": datetime.now()}
