.PHONY: all clean go

BUILD_DIR := build
LIB_NAME  := libhammdist.so
GO_BIN    := marsbotgo

CC      ?= gcc
CFLAGS  ?= -O2 -fPIC -g
LDFLAGS ?= -shared

GO      ?= go
GO_TAGS := sqlite_stat4

all: $(BUILD_DIR)/$(LIB_NAME) go

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/$(LIB_NAME): hammdist/hammdist.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@

go: | $(BUILD_DIR)
	$(GO) build -tags "$(GO_TAGS)" -o $(BUILD_DIR)/$(GO_BIN) ./main

clean:
	rm -rf $(BUILD_DIR)
