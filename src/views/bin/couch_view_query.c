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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../view_group.h"
#include "../util.h"
#include "../file_sorter.h"
#include "util.h"

#define BUF_SIZE 8192

char *create_json_response(result_list_t *rl, int *len);

int main(int argc, char *argv[])
{
    view_group_info_t *group_info = NULL;
    char buf[BUF_SIZE];
    char *tmp_dir = NULL;
    int i;
    int batch_size;
    int ret = 2;
    int is_sorted = 0;
    sized_buf header_buf = {NULL, 0};
    result_list_t *rl;
    view_error_t error_info = {NULL, NULL};
    node_pointer *np = NULL;
    //cb_thread_t exit_thread;

    (void) argc;
    (void) argv;

    /*
     * Disable buffering for stdout since index updater messages
     * needs to be immediately available at erlang side
     */
    setvbuf(stdout, (char *) NULL, _IONBF, 0);

    if (set_binary_mode() < 0) {
        fprintf(stderr, "Error setting binary mode\n");
        goto out;
    }

    group_info = couchstore_read_view_group_info(stdin, stderr);
    if (group_info == NULL) {
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    header_buf.size = couchstore_read_int(stdin, buf, sizeof(buf), &ret);
    if (ret != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Error reading view header size\n");
        goto out;
    }

    header_buf.buf = malloc(header_buf.size);
    if (header_buf.buf == NULL) {
            fprintf(stderr, "Memory allocation failure\n");
            ret = COUCHSTORE_ERROR_ALLOC_FAIL;
            goto out;
    }

    if (fread(header_buf.buf, header_buf.size, 1, stdin) != 1) {
        fprintf(stderr,
                "Error reading viewgroup header from stdin\n");
        goto out;
    }

    sized_buf keys[2];
    char start_key[30], end_key[30];
    *(uint16_t *) start_key = (uint16_t) 8;
    sprintf(start_key + 2, "\"ttt\"");
    keys[0].buf = start_key;
    keys[0].size = 8;

    *(uint16_t *) end_key = (uint16_t) 6;
    sprintf(end_key + 2, "\"zzz1\"");
    keys[1].buf = end_key;
    keys[1].size = 6;

    view_query_request_t query_req = {keys, 2};

    //ret = start_exit_listener(&exit_thread);
    //if (ret) {
    //    fprintf(stderr, "Error starting stdin exit listener thread\n");
    //    goto out;
    //}

    ret = couchstore_handle_query_request(group_info,
                                          &header_buf,
                                          &rl,
                                          &query_req,
                                          &error_info);

    int len = 0;
    char *json_resp = create_json_response(rl, &len);

    if (ret != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Error in handling the query request %d\n", ret);
        goto out;
    }

    fprintf(stderr, "Length %lu\n", len);
    //printf("%.*s", len, json_resp);
    fwrite(json_resp, len, 1, stdout);
    fprintf(stderr, "client write done");
out:
    couchstore_free_view_group_info(group_info);
    free((void *) error_info.error_msg);
    free((void *) error_info.view_name);
    free((void *) header_buf.buf);
    return (ret < 0) ? (100 + ret) : ret;
}

char *get_json_string(result_list_t *p, int *l) {
    char *buff = malloc(p->k->size + 100);
    if (buff == NULL) {
        fprintf(stderr, "out of memory");
    }
    //int len = sprintf(buff, "{\"key\": %.*s; \"value\" : %.*s}", p->k->size,
    //    p->k->buf);
    //
    int len = sprintf(buff, "{\"key\": %.*s; \"value\" : %.*s}", p->k->size,
        p->k->buf, p->v->size, p->v->buf);
    *l = len;
    return buff;
}

char *create_json_response(result_list_t *rl, int *len) {
    int length = 0;
    static char buffer[25000], *b = buffer;
    int totlen = 0;
    while (rl) {
        int len  = 0;
        char *p = get_json_string(rl, &len);
        //*(uint16_t *) b = len;
        *(char *) b = len;
        totlen += len + 1;
        memcpy(b + 1, p, len);
        b = b + len + 1;
        rl = rl->next;
    }
    *len = totlen;
    return buffer;
}


