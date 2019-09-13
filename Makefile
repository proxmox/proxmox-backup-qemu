PACKAGE=libproxmox-backup-qemu-dev
PKGVER=0.1
PKGREL=1

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
GITVERSION:=$(shell git rev-parse HEAD)

DEB=${PACKAGE}_${PKGVER}-${PKGREL}_${ARCH}.deb

ifeq ($(BUILD_MODE), release)
CARGO_BUILD_ARGS += --release
endif

all:
ifneq ($(BUILD_MODE), skip)
	cargo build $(CARGO_BUILD_ARGS)
endif

# always re-create this dir
# but also copy the local target/ dir as a build-cache
.PHONY: build
build:
	rm -rf build
	cargo build --release
	rsync -a debian Makefile Cargo.toml Cargo.lock build.rs proxmox-backup-qemu.h src target build/

.PHONY: deb
deb: $(DEB)
$(DEB): build
	cd build; dpkg-buildpackage -b -us -uc --no-pre-clean
	lintian $(DEB)

test: test.c proxmox-backup-qemu.h
	gcc test.c -o test -lc  -Wl,-rpath=./target/debug -L ./target/debug/ -l proxmox_backup_qemu

distclean: clean

clean:
	cargo clean
	rm -rf *.deb *.dsc *.tar.gz *.buildinfo *.changes Cargo.lock proxmox-backup-qemu.h  build
	find . -name '*~' -exec rm {} ';'

.PHONY: dinstall
dinstall: ${DEB}
	dpkg -i ${DEB}
