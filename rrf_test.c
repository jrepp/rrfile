#include <stdio.h>
#include <string.h>
#include "rrfile.h"


void error_print(rrf_error_t *err) {
    printf("ERROR: %u: %s @%s:%ld\n", err->code, err->message, err->file, err->line);
}

void show_errors(rrf_handle_t *h) {
    rrf_error_t err;
    while(rrf_error(h, &err, 1)) {
        error_print(&err);
    }
}

int main(int argc, char **argv) {
    rrf_handle_t *h = NULL;

    if(!rrf_create(&h, "./test", 8, 16 * 1024)) {
        show_errors(h);
        return -1;
    }
    int max_writes = 10000000;
    int writes = 0;
    char buffer[32] = "blahblahblah";
    int len = strlen(buffer);
    int status;

    while(writes++ < max_writes) {
        // completely fill the write buffer
        do {
            status = rrf_write_async(h, buffer, len, NULL);
        } while(status);

        // spool errors, there should be one write buffer full error
        do {
            rrf_error_t err;
            status = rrf_error(h, &err, 1);
            if(err.code != RRF_WRITE_BUFFER_FULL) {
                error_print(&err);
                goto cleanup;
            }
        } while(status);

        // give the handle thread time
        do {
            status = rrf_service(h);
        } while(status);
    } // end write loop

cleanup:
    if(!rrf_close(&h)) {
        show_errors(h);
    }

    printf("wrote %u record bytes\n", len * writes);
    return 0;
}
