"""流水线入口:python -m pipeline.cli <command>"""

import argparse
import json
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


def main():
    ap = argparse.ArgumentParser(prog="pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("sync-metadata", help="元数据同步 + schema 快照(每日)")
    sub.add_parser("collect-sql", help="调度平台 SQL 增量收集(每日)")
    sub.add_parser("full-rebuild", help="全量血缘重建 + 闭包切换(每日凌晨)")
    p_incr = sub.add_parser("incremental", help="单条 SQL 增量解析(发布钩子)")
    p_incr.add_argument("--sql-id", type=int, required=True)
    sub.add_parser("coverage", help="覆盖率大盘报表")
    sub.add_parser("vectorize", help="向量构建:glossary+字段注释 → Chroma 双 collection(6.4)")
    sub.add_parser("healthcheck", help="健康检查(cron 用,异常退出码非0触发告警)")
    sub.add_parser("weekly-report", help="运营周报 markdown(每周一 cron)")
    args = ap.parse_args()

    from . import db, rebuild
    if args.cmd == "sync-metadata":
        from . import metadata_sync
        metadata_sync.sync(db.connect())
    elif args.cmd == "collect-sql":
        from . import sql_collector
        conn = db.connect()
        for item in sql_collector.fetch_from_scheduler():
            sql_id, changed = sql_collector.upsert_sql(conn, item)
            if changed:
                print(json.dumps(rebuild.incremental(sql_id), ensure_ascii=False))
    elif args.cmd == "full-rebuild":
        print(json.dumps(rebuild.full_rebuild(), ensure_ascii=False))
    elif args.cmd == "incremental":
        print(json.dumps(rebuild.incremental(args.sql_id), ensure_ascii=False))
    elif args.cmd == "coverage":
        from . import coverage
        print(json.dumps(coverage.report(db.connect()), ensure_ascii=False, default=str))
    elif args.cmd == "vectorize":
        from . import vectorize
        print(json.dumps(vectorize.run(db.connect()), ensure_ascii=False))
    elif args.cmd == "healthcheck":
        import sys

        from . import health
        report = health.check(db.connect())
        print(json.dumps(report, ensure_ascii=False, default=str))
        sys.exit(0 if report["healthy"] else 1)
    elif args.cmd == "weekly-report":
        from . import report
        print(report.weekly(db.connect()))


if __name__ == "__main__":
    main()
