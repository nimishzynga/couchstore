#include <stdio.h>
#include "http_parser.h"
#include "jsmn.h"

http_parser_settings *settings;

parser_data *init_http_parser(callback cb, complete_callback ccb, void *conn) {
    parser_data *ps = calloc(1, sizeof(parser_data));
    if (settings == NULL) {
        settings = calloc(1, sizeof(http_parser_settings));
        settings->on_body = cb;
        settings->on_message_complete = ccb;
    }
    http_parser *parser = malloc(sizeof(http_parser));
    http_parser_init(parser, HTTP_RESPONSE);
    parser->data = conn;
    ps->parser = parser;
    return ps;
}

int parse_data(char *buff, int len, parser_data *ps) {
    //fprintf(stderr, "parse data called with buffer %.*s", len, buff);
    //fprintf(stderr, "parser1 is %u\n", ps->parser);
    http_parser_execute(ps->parser, settings, buff, len);
    return 0;
}
