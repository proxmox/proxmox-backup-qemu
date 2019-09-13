#include "stdio.h"
#include "stdlib.h"
#include "proxmox-backup-qemu.h"

void test_cb(void *data) {
    printf("callback called\n");
}

void main(void) {

  printf("This is a test\n");
  ProxmoxBackupHandle *pbs = proxmox_backup_connect();

  printf("Start write\n");
  proxmox_backup_write_data_async(pbs, NULL, 0, test_cb, NULL);

  printf("Join\n");
  proxmox_backup_disconnect(pbs);

  printf("Done\n");
  exit(0);
}
