

ifeq ($(BUILD_MODE), release)
CARGO_BUILD_ARGS += --release
endif

all:
	cargo build $(CARGO_BUILD_ARGS)
