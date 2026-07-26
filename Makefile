.PHONY: init-db up down test

init-db:  ## 按序执行 sql/ 下全部 DDL
	for f in sql/0*.sql; do \
	  echo "== $$f"; \
	  docker compose exec -T mysql mysql -u root -p$$MYSQL_ROOT_PASSWORD datavein < $$f; \
	done

up:
	docker compose up -d

down:
	docker compose down

test:
	cd lineage-pipeline && python -m pytest -q
