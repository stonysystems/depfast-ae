all:
	@echo $(PWD)
	python3 waf configure build -d --db-checksum
