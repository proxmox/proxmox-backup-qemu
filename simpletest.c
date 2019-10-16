#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "proxmox-backup-qemu.h"

void test_cb(void *data) {
  printf("callback called\n");
}

void main(int argc, char **argv) {

  if (argc != 2) {
    fprintf(stderr, "usage: simpletest <repository>\n");
    exit(-1);
  }

  const char *repository = argv[1];

  const char *backup_id = "99";
  uint64_t backup_time = time(NULL);

  char *pbs_error = NULL;

  ProxmoxBackupHandle *pbs = proxmox_backup_connect
    (repository, backup_id, backup_time, NULL, NULL, NULL, &pbs_error);

  if (pbs == NULL) {
    fprintf(stderr, "proxmox_backup_connect failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  int dev_id = proxmox_backup_register_image(pbs, "scsi-drive0", 1024*1024*64, &pbs_error);
  if (dev_id < 0) {
    fprintf(stderr, "proxmox_backup_register_image failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  printf("write a single chunk\n");
  proxmox_backup_write_data_async(pbs, dev_id, NULL, 0, 4*1024*1024, test_cb, NULL, &pbs_error);

  // simply abort now

  printf("Join\n");
  proxmox_backup_disconnect(pbs);

  printf("Done\n");
  exit(0);
}
