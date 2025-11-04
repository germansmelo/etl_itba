.PHONY: run test stop

run:
	docker-compose -f docker/docker-compose.yml up -d

test:
	pytest -q

stop:
	docker-compose -f docker/docker-compose.yml down -v
