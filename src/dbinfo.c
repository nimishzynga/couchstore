/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include <getopt.h>

#include "internal.h"
#include "util.h"
#include "bitfield.h"


static char *size_str(double size)
{
    static char rfs[256];
    int i = 0;
    const char *units[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    while (size > 1024) {
        size /= 1024;
        i++;
    }
    snprintf(rfs, sizeof(rfs), "%.*f %s", i, size, units[i]);
    return rfs;
}

static void print_db_info(Db* db)
{
    DbInfo info;
    couchstore_db_info(db, &info);
    if (info.doc_count == 0) {
        printf("   no documents\n");
        return;
    }
    printf("   doc count: %"PRIu64"\n", info.doc_count);
    printf("   deleted doc count: %"PRIu64"\n", info.deleted_count);
    printf("   data size: %s\n", size_str(info.space_used));
}

static int process_file(const char *file, int iterate_headers)
{
    Db *db;
    couchstore_error_t errcode;
    uint64_t btreesize = 0;

    errcode = couchstore_open_db(file, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
    if (errcode != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Failed to open \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    }

next_header:
    printf("DB Info (%s) - header at %"PRIu64"\n", file, db->header.position);
    printf("   file format version: %"PRIu64"\n", db->header.disk_version);
    printf("   update_seq: %"PRIu64"\n", db->header.update_seq);
    print_db_info(db);
    if (db->header.by_id_root) {
        btreesize += db->header.by_id_root->subtreesize;
    }
    if (db->header.by_seq_root) {
        btreesize += db->header.by_seq_root->subtreesize;
    }
    printf("   B-tree size: %s\n", size_str(btreesize));
    printf("   total disk size: %s\n", size_str(db->file.pos));
    if (iterate_headers) {
        if (couchstore_rewind_db_header(db) == COUCHSTORE_SUCCESS) {
            printf("\n");
            goto next_header;
        }
    } else {
        couchstore_close_db(db);
    }

    return 0;
}

static void usage(const char *name)
{
    fprintf(stderr, "USAGE: %s [-i] <file.couch>\n", name);
    fprintf(stderr, "\t-i Iterate through all headers\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv)
{
    int error = 0;
    int ii;
    int iterate_headers = getenv("ITERATE_HEADERS") != NULL;
    int cmd;

    while ((cmd = getopt(argc, argv, "i")) != -1) {
        switch (cmd) {
        case 'i':
            iterate_headers = 1;
            break;
        default:
            usage(argv[0]);
            /* NOTREACHED */
        }
    }

    if (optind == argc) {
        usage(argv[0]);
    }

    for (ii = optind; ii < argc; ++ii) {
        error += process_file(argv[ii], iterate_headers);
    }

    if (error) {
        exit(EXIT_FAILURE);
    } else {
        exit(EXIT_SUCCESS);
    }
}
