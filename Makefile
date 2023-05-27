include /usr/share/dpkg/default.mk

PACKAGE=libproxmox-backup-qemu0

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
export GITVERSION:=$(shell git rev-parse HEAD)

MAIN_DEB=$(PACKAGE)_$(DEB_VERSION)_$(ARCH).deb
OTHER_DEBS=						\
	$(PACKAGE)-dev_$(DEB_VERSION)_$(ARCH).deb	\
	$(PACKAGE)-dbgsym_$(DEB_VERSION)_$(ARCH).deb
DEBS=$(MAIN_DEB) $(OTHER_DEBS)

DESTDIR=

ifeq ($(BUILD_MODE), release)
CARGO_BUILD_ARGS += --release
endif

all:
ifneq ($(BUILD_MODE), skip)
	cargo build $(CARGO_BUILD_ARGS)
	diff -up current-api.h proxmox-backup-qemu.h
endif

# always re-create this dir
# but also copy the local target/ dir as a build-cache
.PHONY: build
build:
	rm -rf build
	cargo build --release
	diff -I 'PROXMOX_BACKUP_QEMU_VERSION' -up current-api.h proxmox-backup-qemu.h
	rsync -a debian submodules Makefile Cargo.toml Cargo.lock build.rs proxmox-backup-qemu.h src target current-api.h build/

.PHONY: install
install: target/release/libproxmox_backup_qemu.so
	install -D -m 0755 target/release/libproxmox_backup_qemu.so $(DESTDIR)/usr/lib//libproxmox_backup_qemu.so.0
	cd $(DESTDIR)/usr/lib/; ls *; ln -s libproxmox_backup_qemu.so.0 libproxmox_backup_qemu.so

submodule:
	[ -e submodules/proxmox-backup/Cargo.toml ] || git submodule update --init --recursive

.PHONY: deb
deb: $(OTHER_DEBS)
$(OTHER_DEBS): $(MAIN_DEB)
$(MAIN_DEB): build
	cd build; dpkg-buildpackage -b -us -uc --no-pre-clean
	lintian $(DEBS)

proxmox-backup-qemu.h: build

simpletest: simpletest.c proxmox-backup-qemu.h
	gcc simpletest.c -o simpletest -lc  -Wl,-rpath=./target/$(BUILD_MODE) -L ./target/$(BUILD_MODE) -l proxmox_backup_qemu

distclean: clean

clean:
	cargo clean
	rm -rf *.deb *.dsc *.tar.gz *.buildinfo *.changes Cargo.lock proxmox-backup-qemu.h  build

.PHONY: dinstall
dinstall: $(DEBS)
	dpkg -i $(DEBS)

.PHONY: upload
upload: $(DEBS)
	# check if working directory is clean
	git diff --exit-code --stat && git diff --exit-code --stat --staged
	tar cf - $(DEBS) | ssh -X repoman@repo.proxmox.com upload --product pve --dist bullseye
