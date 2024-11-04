TARGET = $(HOME)/.local/bin/snapshot

$(TARGET): snapshot.py
	install -D snapshot.py "$(TARGET)"

all: $(TARGET)
