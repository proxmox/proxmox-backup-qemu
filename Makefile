include /usr/share/dpkg/default.mk

PACKAGE = libproxmox-backup-qemu0
BUILDDIR = $(PACKAGE)-$(DEB_VERSION_UPSTREAM)

ARCH:=$(DEB_HOST_ARCH)
export GITVERSION:=$(shell git rev-parse HEAD)

MAIN_DEB=$(PACKAGE)_$(DEB_VERSION)_$(ARCH).deb
OTHER_DEBS = \
	$(PACKAGE)-dev_$(DEB_VERSION)_$(ARCH).deb \
	$(PACKAGE)-dbgsym_$(DEB_VERSION)_$(ARCH).deb
DEBS=$(MAIN_DEB) $(OTHER_DEBS)

DESTDIR=

TARGETDIR := target/debug

ifeq ($(BUILD_MODE), release)
CARGO_BUILD_ARGS += --release
TARGETDIR := target/release
endif

.PHONY: all build
# source target
all: build

# source target
build: $(TARGETDIR)/libproxmox_backup_qemu.so
$(TARGETDIR)/libproxmox_backup_qemu.so: Cargo.toml src/
	cargo build $(CARGO_BUILD_ARGS)

# source / packaging target
.PHONY: install
install: $(TARGETDIR)/libproxmox_backup_qemu.so
	install -D -m 0755 $(TARGETDIR)/libproxmox_backup_qemu.so $(DESTDIR)/usr/lib/libproxmox_backup_qemu.so.0
	cd $(DESTDIR)/usr/lib/; ls *; ln -s libproxmox_backup_qemu.so.0 libproxmox_backup_qemu.so

.PHONY: test
test: current-api.h proxmox-backup-qemu.h
	diff -I 'PROXMOX_BACKUP_QEMU_VERSION' -up current-api.h proxmox-backup-qemu.h

# packaging target
$(BUILDDIR): submodule
	rm -rf $@ $@.tmp && mkdir $@.tmp
	cp -a submodules debian Makefile .cargo Cargo.toml build.rs src header-preamble.c current-api.h $@.tmp/
	mv $@.tmp $@

submodule:
	[ -e submodules/proxmox-backup/Cargo.toml ] || git submodule update --init --recursive

.PHONY: deb
deb: $(OTHER_DEBS)
$(OTHER_DEBS): $(MAIN_DEB)
$(MAIN_DEB): $(BUILDDIR)
	cd build; dpkg-buildpackage -b -us -uc --no-pre-clean
	lintian $(DEBS)

proxmox-backup-qemu.h: $(TARGETDIR)/libproxmox_backup_qemu.so

simpletest: simpletest.c proxmox-backup-qemu.h
	gcc simpletest.c -o simpletest -lc  -Wl,-rpath=./$(TARGETDIR) -L ./$(TARGETDIR) -l proxmox_backup_qemu

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
