
#ifndef _included_rrfile_h
#define _included_rrfile_h

// codes for rrf api
typedef enum {
    RRF_ERR_CLASS_INT = 1,
    RRF_OPEN_FAILED,
    RRF_MALLOC_FAILED,

    RRF_ERR_CLASS_IO = 10,
    RRF_WRITE_BUFFER_FULL,
    RRF_WRITE_FAILED,
    RRF_SEEK_FAILED,

    RRF_ERR_CLASS_API = 20,
    RRF_INVALID_ARGUMENT
} rrf_code_t;

// opaque file handle
typedef struct rrf_handle_t rrf_handle_t;

// struct describing errors from the library
typedef struct rrf_error_t {
    const char *message;
    rrf_code_t code;
    const char *file;
    long line;
} rrf_error_t;

typedef struct rrf_context_t {
    // handle that invoked the context
    rrf_handle_t *handle;

    // pointer to callback and user data
    void (*callback)(rrf_handle_t *, struct rrf_context_t*);
    void *userdata;

    // return value and size if applicable
    const void *rvalue;
    unsigned size;
} rrf_context_t;


// creates <partitions> count files that cap at the provided size
// returns 1 on success
// returns 0 on failure, see rrf_error
int rrf_create(rrf_handle_t **handle, const char *filepath, unsigned partitions, unsigned size);

// close the handle return
int rrf_close(rrf_handle_t **handle);

// write to the handle synchronous
int rrf_write(rrf_handle_t *handle, const void *record, unsigned size);

// write to the handle asynchronous
int rrf_write_async(rrf_handle_t *handle, const void *record, unsigned size, rrf_context_t *context);

// give thread time to async writes
int rrf_service(rrf_handle_t *handle);

// read last errors from the handle, up to count records copied
int rrf_error(rrf_handle_t *handle, rrf_error_t *errors, unsigned count);


#endif // _included_rrfile_h
