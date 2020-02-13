#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

#include "proxmox-backup-qemu.h"

void main(int argc, char **argv) {

  if (argc != 2) {
    fprintf(stderr, "usage: simpletest <repository>\n\n");
    fprintf(stderr, "uses environment vars: PBS_PASSWORD and PBS_FINGERPRINT\n");
    exit(-1);
  }

  const char *repository = argv[1];

  const char *backup_id = "99";
  uint64_t backup_time = time(NULL);

  char *pbs_error = NULL;

  char *password = getenv("PBS_PASSWORD");

  char *fingerprint = getenv("PBS_FINGERPRINT");

  ProxmoxBackupHandle *pbs = proxmox_backup_new
    (repository, backup_id, backup_time, PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE, password, NULL, NULL, fingerprint, &pbs_error);

  if (pbs == NULL) {
    fprintf(stderr, "proxmox_backup_new failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  printf("connect\n");
  if (proxmox_backup_connect(pbs, &pbs_error) != 0) {
    fprintf(stderr, "proxmox_backup_connect failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  printf("add config\n");
  char *config_data = "key1: value1\nkey2: value2\n";
  if (proxmox_backup_add_config(pbs, "test.conf", config_data, strlen(config_data), &pbs_error) != 0) {
    fprintf(stderr, "proxmox_backup_connect failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }


  int img_chunks = 16;

  printf("register_image\n");
  int dev_id = proxmox_backup_register_image(pbs, "scsi-drive0", PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE*img_chunks, &pbs_error);
  if (dev_id < 0) {
    fprintf(stderr, "proxmox_backup_register_image failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  for (int i = 0; i < img_chunks; i++) {
    printf("write a single chunk %d\n", i);
    proxmox_backup_write_data(pbs, dev_id, NULL, i*PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE, PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE, &pbs_error);
  }

  printf("close_image\n");
  if (proxmox_backup_close_image(pbs, dev_id, &pbs_error) < 0) {
    fprintf(stderr, "proxmox_backup_close_image failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  printf("finish backup\n");
  if (proxmox_backup_finish(pbs, &pbs_error) < 0) {
    fprintf(stderr, "proxmox_backup_finish failed - %s\n", pbs_error);
    proxmox_backup_free_error(pbs_error);
    exit(-1);
  }

  // simply abort now

  printf("join\n");
  proxmox_backup_disconnect(pbs);

  printf("Done\n");
  exit(0);
}
