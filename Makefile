PACKAGE=libproxmox-backup-qemu0
PKGVER=0.1
PKGREL=1

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
GITVERSION:=$(shell git rev-parse HEAD)

DEBS=${PACKAGE}_${PKGVER}-${PKGREL}_${ARCH}.deb ${PACKAGE}-dev_${PKGVER}-${PKGREL}_${ARCH}.deb

DESTDIR=

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

.PHONY: install
install: target/release/libproxmox_backup_qemu.so
	install -D -m 0755 target/release/libproxmox_backup_qemu.so ${DESTDIR}/usr/lib//libproxmox_backup_qemu.so.0
	ln -s ${DESTDIR}/usr/lib//libproxmox_backup_qemu.so.0 ${DESTDIR}/usr/lib//libproxmox_backup_qemu.so

.PHONY: deb
deb: $(DEBS)
$(DEBS): build
	cd build; dpkg-buildpackage -b -us -uc --no-pre-clean
	lintian $(DEBS)

simpletest: simpletest.c proxmox-backup-qemu.h
	gcc simpletest.c -o simpletest -lc  -Wl,-rpath=./target/$(BUILD_MODE) -L ./target/$(BUILD_MODE) -l proxmox_backup_qemu

distclean: clean

clean:
	cargo clean
	rm -rf *.deb *.dsc *.tar.gz *.buildinfo *.changes Cargo.lock proxmox-backup-qemu.h  build
	find . -name '*~' -exec rm {} ';'

.PHONY: dinstall
dinstall: ${DEBS}
	dpkg -i ${DEBS}
