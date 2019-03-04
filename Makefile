PREFIX=$(HOME)

install:
	mkdir -p $(PREFIX)/bin
	cp logshard_server.py $(PREFIX)/bin
	cp logshard_client.py $(PREFIX)/bin

