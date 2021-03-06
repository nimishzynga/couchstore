/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Sarath Lakshman  <sarath@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "config.h"
#include "../view_group.h"
#include "../util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

#define BUF_SIZE 8192

static void die_on_exit_msg(void *args)
{
    char buf[4];
    (void) args;

    if (fread(buf, 1, 4, stdin) == 4 && !strncmp(buf, "exit", 4)) {
        exit(1);
    }
}

int main(int argc, char *argv[])
{
    view_group_info_t *group_info = NULL;
    uint64_t purge_count;
    int ret = 1;
    uint64_t header_pos;
    view_error_t error_info;
    cb_thread_t exit_thread;

    (void) argc;
    (void) argv;

    group_info = couchstore_read_view_group_info(stdin, stderr);
    if (group_info == NULL) {
        ret = 1;
        goto out;
    }

    /* Start a watcher thread to gracefully die on exit message */
    ret = cb_create_thread(&exit_thread, die_on_exit_msg, NULL, 1);
    if (ret < 0) {
        fprintf(stderr, "Error starting stdin exit listener thread\n");
        goto out;
    }

    ret = couchstore_cleanup_view_group(group_info,
                                        &header_pos,
                                        &purge_count,
                                        &error_info);

    if (ret != COUCHSTORE_SUCCESS) {
        if (error_info.error_msg != NULL && error_info.view_name != NULL) {
            fprintf(stderr,
                    "Error cleaning up index for view `%s`, reason: %s\n",
                    error_info.view_name,
                    error_info.error_msg);
        }
        goto out;
    }

    fprintf(stdout, "PurgedCount %"PRIu64"\n", purge_count);

out:
    couchstore_free_view_group_info(group_info);
    free((void *) error_info.error_msg);
    free((void *) error_info.view_name);

    return (ret < 0) ? (100 + ret) : ret;
}
