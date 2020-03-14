-include .env
export

APP_NAME		:= sqlloadetl
PYTHON_BIN		?= python
PIP_BIN			?= pip
DATA_DIR		?= data

.PHONY: pyversion
pyversion:
	$(PYTHON_BIN) --version

.PHONY: test
test:
	cd app; $(PYTHON_BIN) -m unittest -v

.PHONY: prerequisites
prerequisites:
	$(PIP_BIN) install -r requirements.txt

.PHONY: localrun
localrun:
	cd app; $(PYTHON_BIN) main.py

.PHONY: localhelp
localhelp:
	cd app; $(PYTHON_BIN) main.py -h

.PHONY: build
build:
	docker build -t $(APP_NAME) .

.PHONY: run
run:
	docker run --rm --name $(APP_NAME) -v $(DATA_DIR):/data --env-file docker.env $(APP_NAME)

.PHONY: sqsmock-build
sqsmock-build:
	cd sqs-mock; docker build -t sqs-mock .

.PHONY: sqsmock-run
sqsmock-run:
	cd sqs-mock; docker run --rm --name sqs-mock -p 9324:9324 -d sqs-mock
