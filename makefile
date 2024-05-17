lint: staticcheck
	staticcheck -checks=all ./...

staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

run: setup
	go run .

db:
	docker compose -f compose.yaml up -d db

source:
	docker compose -f compose.yaml up -d source

tear:
	docker compose -f compose.yaml down

setup: db source

tidy:
	go mod tidy
	go mod vendor
