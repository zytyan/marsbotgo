.PHONY: all clean

BUILD_DIR := build
LIB_NAME  := libhammdist.so
GO_BIN    := marsbotgo

CC      ?= gcc
CFLAGS  ?= -O2 -fPIC -g
LDFLAGS ?= -shared

GO      ?= go
GO_TAGS := sqlite_stat4

all: $(BUILD_DIR)/$(LIB_NAME) $(BUILD_DIR)/$(GO_BIN)

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/$(LIB_NAME): hammdist/hammdist.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) $< -o $@

$(BUILD_DIR)/$(GO_BIN): | $(BUILD_DIR)
	$(GO) build -tags "$(GO_TAGS)" -o $@ .

clean:
	rm -rf $(BUILD_DIR)
