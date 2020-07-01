/*
 * A Proxmox Backup Server C interface, intended for use inside Qemu
 *
 * Copyright (C) 2019 Proxmox Server Solutions GmbH
 *
 * Authors:
 *  Dietmar Maurer (dietmar@proxmox.com)
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 *
 *
 * NOTE: Async Commands
 *
 * Most commands are asynchronous (marked as _async). They run in a
 * separate thread and have the following parameters:
 *
 * callback: extern "C" fn(*mut c_void),
 * callback_data: *mut c_void,
 * result: *mut c_int,
 * error: *mut *mut c_char,
 *
 * The callback function is called when the the async function is
 * ready. Possible errors are returned in 'error'.
 */


#ifndef PROXMOX_BACKUP_QEMU_H
#define PROXMOX_BACKUP_QEMU_H

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE ((1024 * 1024) * 4)

/**
 * Opaque handle for backups jobs
 */
typedef struct {

} ProxmoxBackupHandle;

/**
 * Opaque handle for restore jobs
 */
typedef struct {

} ProxmoxRestoreHandle;

/**
 * Abort a running backup task
 *
 * This stops the current backup task. It is still necessary to call
 * proxmox_backup_disconnect() to close the connection and free
 * allocated memory.
 */
void proxmox_backup_abort(ProxmoxBackupHandle *handle, const char *reason);

/**
 * Add a configuration blob to the backup (sync)
 */
int proxmox_backup_add_config(ProxmoxBackupHandle *handle,
                              const char *name,
                              const uint8_t *data,
                              uint64_t size,
                              char **error);

/**
 * Add a configuration blob to the backup
 *
 * Create and upload a data blob "<name>.blob".
 */
void proxmox_backup_add_config_async(ProxmoxBackupHandle *handle,
                                     const char *name,
                                     const uint8_t *data,
                                     uint64_t size,
                                     void (*callback)(void*),
                                     void *callback_data,
                                     int *result,
                                     char **error);

/**
 * Close a registered image (sync)
 */
int proxmox_backup_close_image(ProxmoxBackupHandle *handle, uint8_t dev_id, char **error);

/**
 * Close a registered image
 *
 * Mark the image as closed. Further writes are not possible.
 */
void proxmox_backup_close_image_async(ProxmoxBackupHandle *handle,
                                      uint8_t dev_id,
                                      void (*callback)(void*),
                                      void *callback_data,
                                      int *result,
                                      char **error);

/**
 * Open connection to the backup server (sync)
 *
 * Returns:
 *  0 ... Sucecss (no prevbious backup)
 *  1 ... Success (found previous backup)
 * -1 ... Error
 */
int proxmox_backup_connect(ProxmoxBackupHandle *handle, char **error);

/**
 * Open connection to the backup server
 *
 * Returns:
 *  0 ... Sucecss (no prevbious backup)
 *  1 ... Success (found previous backup)
 * -1 ... Error
 */
void proxmox_backup_connect_async(ProxmoxBackupHandle *handle,
                                  void (*callback)(void*),
                                  void *callback_data,
                                  int *result,
                                  char **error);

/**
 * Disconnect and free allocated memory
 *
 * The handle becomes invalid after this call.
 */
void proxmox_backup_disconnect(ProxmoxBackupHandle *handle);

/**
 * Finish the backup (sync)
 */
int proxmox_backup_finish(ProxmoxBackupHandle *handle, char **error);

/**
 * Finish the backup
 *
 * Finish the backup by creating and uploading the backup manifest.
 * All registered images have to be closed before calling this.
 */
void proxmox_backup_finish_async(ProxmoxBackupHandle *handle,
                                 void (*callback)(void*),
                                 void *callback_data,
                                 int *result,
                                 char **error);

/**
 * Free returned error messages
 *
 * All calls can return error messages, but they are allocated using
 * the rust standard library. This call moves ownership back to rust
 * and free the allocated memory.
 */
void proxmox_backup_free_error(char *ptr);

/**
 * Create a new instance
 *
 * Uses `PROXMOX_BACKUP_DEFAULT_CHUNK_SIZE` if `chunk_size` is zero.
 */
ProxmoxBackupHandle *proxmox_backup_new(const char *repo,
                                        const char *backup_id,
                                        uint64_t backup_time,
                                        uint64_t chunk_size,
                                        const char *password,
                                        const char *keyfile,
                                        const char *key_password,
                                        const char *fingerprint,
                                        char **error);

/**
 * Register a backup image (sync)
 */
int proxmox_backup_register_image(ProxmoxBackupHandle *handle,
                                  const char *device_name,
                                  uint64_t size,
                                  bool incremental,
                                  char **error);

/**
 * Register a backup image
 *
 * Create a new image archive on the backup server
 * ('<device_name>.img.fidx'). The returned integer is the dev_id
 * parameter for the proxmox_backup_write_data_async() method.
 */
void proxmox_backup_register_image_async(ProxmoxBackupHandle *handle,
                                         const char *device_name,
                                         uint64_t size,
                                         bool incremental,
                                         void (*callback)(void*),
                                         void *callback_data,
                                         int *result,
                                         char **error);

/**
 * Write data to into a registered image (sync)
 */
int proxmox_backup_write_data(ProxmoxBackupHandle *handle,
                              uint8_t dev_id,
                              const uint8_t *data,
                              uint64_t offset,
                              uint64_t size,
                              char **error);

/**
 * Write data to into a registered image
 *
 * Upload a chunk of data for the <dev_id> image.
 */
void proxmox_backup_write_data_async(ProxmoxBackupHandle *handle,
                                     uint8_t dev_id,
                                     const uint8_t *data,
                                     uint64_t offset,
                                     uint64_t size,
                                     void (*callback)(void*),
                                     void *callback_data,
                                     int *result,
                                     char **error);

/**
 * Simple interface to restore images
 *
 * Connect the the backup server.
 *
 * Note: This implementation is not async
 */
ProxmoxRestoreHandle *proxmox_restore_connect(const char *repo,
                                              const char *snapshot,
                                              const char *password,
                                              const char *keyfile,
                                              const char *key_password,
                                              const char *fingerprint,
                                              char **error);

/**
 * Disconnect and free allocated memory
 *
 * The handle becomes invalid after this call.
 */
void proxmox_restore_disconnect(ProxmoxRestoreHandle *handle);

/**
 * Restore an image
 *
 * Image data is downloaded and sequentially dumped to the callback.
 */
int proxmox_restore_image(ProxmoxRestoreHandle *handle,
                          const char *archive_name,
                          int (*callback)(void*, uint64_t, const unsigned char*, uint64_t),
                          void *callback_data,
                          char **error,
                          bool verbose);

#endif /* PROXMOX_BACKUP_QEMU_H */
