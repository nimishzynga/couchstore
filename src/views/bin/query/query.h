#ifndef _QUERY_H_
#define _QUERY_H_
#include <event.h>
#include "http_parser.h"
#include "json_parse.h"
#include "../../../couch_btree.h"

typedef struct {
    char *key;
    char *data;
} row;

typedef struct {
} queue;

typedef enum {
    success,
    failure,
} status_code;

typedef enum {
    task_send_request,
    task_read_socket,
    task_write_socket,
    task_create_result,
    task_free,
    task_local_query
} task_type;

typedef struct _send_request{
    char *url;
    char *server;
    int port; // if port is 0, it is local request
    node_pointer *np;
    char *filepath;
    struct event_base *base;
    _send_request *next;
} send_request;

typedef struct {
    send_request *sr;
    int size;
} main_task_t;

typedef struct {
    int limit;
    int stale;
} query_args;

typedef struct {
    task_type type;
    void *task_data;
} task;

typedef struct {
    void *args[3];
} query_params;

#define MAX_BUFF 40240
#define MAX_READ_BUFF 2048
typedef struct conn {
     int fd;
     task_type state;
     struct event event;
     char write_buffer[MAX_BUFF];
     char json_buffer[MAX_BUFF];
     int write_size;
     int json_size;
     parser_data *parser;
     struct conn *next;
     short which;
     row_list_t query_row;
     cb_cond_t row_cond;
     cb_mutex_t row_mutex;
     bool query_end;
     void *data;
} conn;

typedef struct {
    conn **c;
    int size;
} sort_task_t;

typedef struct thread_task{
    conn *c;
    struct thread_task *next;
} thread_task;

typedef struct {
    thread_task *head;
    thread_task *last;
    cb_cond_t full;
    cb_mutex_t queue_lock;
    int item_count;
    int thread_id;
} thread_task_queue;

#define DEBUG(format, ...) fprintf(stderr, format, __VA_ARGS__)
#define DEBUG_STR(str) fprintf(stderr, str)
#define PANIC(msg)  {perror(msg); abort();}
typedef status_code (*task_callback)(void *arg);
typedef void (*thread_func)(void *);
int parse_json(http_parser *parser, const char *at, size_t len);
int message_complete(http_parser *parser);
row_t *get_row(conn *);

conn *send_request_to_server(send_request *sreq);
status_code read_data(void *arg);
status_code create_result(void *arg);

int query_start();
int query_dispatch(main_task_t *sr);
void dispatch_local_task(conn *c);
main_task_t *parse_query_args(char *data, int len);
char *read_query_data(int& len);

#endif
