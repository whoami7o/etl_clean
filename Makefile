up:
	docker-compose -f docker-compose.yml up --build -d
down:
	docker-compose -f docker-compose.yml down -v
