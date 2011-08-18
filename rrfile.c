#include "platform.h"
#include "rrfile.h"

// file structure:
//
// header - sig, last known record start, partitions, size, write_index
//
// footer - sig, last known record start, partitions, size, write_index
//
// all atoms in header and footer are 4 byte unsigned
// write_index is running counter of records written 
//
// sig, last record start (abs seek pos) (4byte)

// constants
#define RRF_SIG_V1 (('r' << 24) | ('r' << 16) | ('f' << 8) | 0x01)
#define RRF_FILEPATH_MAX 1024
#define RRF_WRITE_BLOCK_SIZE 4096
#define RRF_ERRORS_MAX 16
#define RRF_WRITE_BLOCK_CACHE 8 // in units of write block size
#define RRF_MIN(a, b) ((a) < (b) ? (a) : (b))
#define RRF_MAX(a, b) ((a) > (b) ? (a) : (b))

// helper macros
#define RRF_ERROR(h, code, msg) rrf_add_error(h, code, msg, __FILE__, __LINE__)
#define RRF_ABORT(msg) fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, msg); abort();

// internal types
typedef struct rrf_write_block_t {
    struct rrf_write_block_t *next;
    unsigned count;
    unsigned written;
    char buffer[RRF_WRITE_BLOCK_SIZE];
} rrf_write_block_t;

// handle type defined through opaque pointer in header
struct rrf_handle_t {
    FILE *current_file;
    //FILE *next_file;
    unsigned write_index;
    unsigned file_index;
    unsigned current_file_bytes;
    unsigned last_record_start;

    char filepath[RRF_FILEPATH_MAX];
    unsigned partitions;
    unsigned size;

    rrf_write_block_t *pending; 
    rrf_write_block_t *current;
    rrf_context_t *context;

    unsigned errors;
    rrf_error_t errorbuf[RRF_ERRORS_MAX];
};

// api data area
static rrf_write_block_t *rrf_block_cache_head;
static rrf_write_block_t *rrf_block_cache_buffer;

// count of active handles, cache active when > 0
static unsigned rrf_handles_active;

// if error happens without a handle to write to
static rrf_error_t rrf_sys_error; 

//
// internal api
//

// add an error
static void rrf_add_error(rrf_handle_t *h, rrf_code_t code, const char *message, const char *file, long line) {
    rrf_error_t *e;
    if(h && h->errors < RRF_ERRORS_MAX) {
        e = &h->errorbuf[h->errors];
        h->errors++;
    }
    else {
        e = &rrf_sys_error;
    }

    e->code = code;
    e->message = message;
    e->file = file;
    e->line = line;
}

static int rrf_safe_seek(rrf_handle_t *h, FILE *file, long pos) {
    if(fseek(file, pos, SEEK_SET) != 0) {
        RRF_ERROR(h, RRF_SEEK_FAILED, strerror(errno));
        return 0;
    }
    return 1;
}

static int rrf_stamp(rrf_handle_t *h, FILE *stream) {
    char header[16];
    unsigned int *write = (unsigned int*)header;
    *write = RRF_SIG_V1; write++;           // sig
    *write = h->partitions; write++;        // partitions
    *write = h->size; write++;              // size
    *write = h->write_index;                // write_index

    if(sizeof(header) != fwrite(header, 1, sizeof(header), stream)) {
        RRF_ERROR(h, RRF_WRITE_FAILED, strerror(errno));
        return 0;
    }

    h->current_file_bytes += sizeof(header);
    h->last_record_start = sizeof(header);

    return 1;
}

static FILE* rrf_check_or_open(rrf_handle_t *h, FILE *file, unsigned file_index) {
    char pattern[RRF_FILEPATH_MAX];
    if(file) return file;
    if(!file) {
        snprintf(pattern, sizeof(pattern), "%s.%u", h->filepath, file_index);
        file = fopen(pattern, "wb+");
    }
    if(file) {
        if(!rrf_safe_seek(h, file, 0)) return NULL;
        if(!rrf_stamp(h, file)) return NULL;
        return file;
    }
    return NULL;
}

// make sure current and next file handles open
static int rrf_ensure_file_handles(rrf_handle_t *h) {
    //if(h->current_file && h->next_file) {
    if(h->current_file) {
        return 1;
    }

    // open current file, seek to 0 and stamp
    h->current_file = rrf_check_or_open(h, h->current_file, h->file_index);
    //h->next_file = rrf_check_or_open(h, h->next_file, (h->file_index + 1) % h->partitions);
    //if(!h->current_file || !h->next_file) {
    if(!h->current_file) {
        RRF_ERROR(h, RRF_OPEN_FAILED, h->filepath);
        return 0;
    }

    return 1;
}

// swap next with current, ensure current
static int rrf_roll_file_handles(rrf_handle_t *h) {
    if(h->current_file && (h->current_file_bytes >= h->size)) {
        rrf_stamp(h, h->current_file);
        fclose(h->current_file);
        //h->current_file = h->next_file;
        //h->next_file = NULL;
        h->current_file = NULL;
        h->current_file_bytes = 0;
        h->file_index = ((h->file_index + 1) % h->partitions);
        return rrf_ensure_file_handles(h);
    }
    return 1;
}


static rrf_write_block_t *rrf_get_write_block(rrf_handle_t *h, unsigned size, int fit) {
#define RRF_FIT 1
#define RRF_ANY 0
   // use the current block if fits
   if(h->current && h->current->count < RRF_WRITE_BLOCK_SIZE) {
       // user requests fit (doesn't want to handle boundaries)
       if(fit && size < (RRF_WRITE_BLOCK_SIZE - h->current->count)) {
           return h->current; 
       }
   }

   rrf_write_block_t *block = rrf_block_cache_head;
   if(block) {
       rrf_block_cache_head = rrf_block_cache_head->next;

       block->next = h->pending;
       block->count = 0;
       block->written = 0;
       h->pending = block;
   }

   h->current = block;

   return block;
}

// validate the rrf handle
static int rrf_check_api_handle(const rrf_handle_t **handle) {
    if(!handle || !*handle) {
        rrf_sys_error.code = RRF_INVALID_ARGUMENT;
        rrf_sys_error.message = "handle";
        rrf_sys_error.file = __FILE__;
        rrf_sys_error.line = __LINE__;
        return 0;
    }
    return 1;
}


// check argument and add error
static int rrf_arg_not_null(rrf_handle_t *h, const void *ptr, const char *name) {
    if(!ptr) {
        RRF_ERROR(h, RRF_INVALID_ARGUMENT, name);
        return 0;
    }
    return 1;
};

//
// public api
//

int rrf_create(rrf_handle_t **handle, const char *filepath, unsigned partitions, unsigned size)
{
    if(!handle) return 0;
    if(!rrf_arg_not_null(NULL, handle, "handle")) return 0;

    // initialize the cache buffer, todo: atomic
    if(!rrf_block_cache_buffer) {
        rrf_block_cache_buffer = (rrf_write_block_t*)malloc(sizeof(rrf_write_block_t) * RRF_WRITE_BLOCK_SIZE);
        if(!rrf_block_cache_buffer) {
            rrf_sys_error.code = RRF_MALLOC_FAILED;
            rrf_sys_error.message = "malloc()";
            rrf_sys_error.file = __FILE__;
            rrf_sys_error.line = __LINE__;
            return 0;
        }

        // populate the block cache head (memory uninitialized)
        rrf_write_block_t *block = rrf_block_cache_buffer, *term = block + RRF_WRITE_BLOCK_CACHE;
        for(; block != term; ++block) {
            block->next = rrf_block_cache_head;
            rrf_block_cache_head = block;
        }
    }

    rrf_handle_t *h = (rrf_handle_t*)malloc(sizeof(rrf_handle_t));
    if(!h) {
        rrf_sys_error.code = RRF_MALLOC_FAILED;
        rrf_sys_error.message = "malloc()";
        rrf_sys_error.file = __FILE__;
        rrf_sys_error.line = __LINE__;
        return 0;
    }

    memset(h, 0, sizeof(rrf_handle_t));

    strncpy(h->filepath, filepath, RRF_FILEPATH_MAX);
    h->partitions = partitions;
    h->size = size;

    if(!rrf_ensure_file_handles(h)) { 
        free(h);
        return 0;
    }

    *handle = h;
    rrf_handles_active++;


    return 1;
}

int rrf_close(rrf_handle_t **handle)
{
    if(!rrf_check_api_handle((const rrf_handle_t**)handle)) return 2;
    rrf_handle_t *h = *handle;
    while(rrf_service(h)) {
        ;;
    }

    fclose(h->current_file);
    //fclose(h->next_file);
    rrf_handles_active--;

    if(rrf_handles_active == 0) {
        free(rrf_block_cache_buffer);
        rrf_block_cache_buffer = NULL;
        rrf_block_cache_head = NULL;
    }

    return 0;
}

int rrf_write(rrf_handle_t *h, const void *record, unsigned size)
{
    if(!rrf_write_async(h, record, size, NULL)) {
        return 0;
    }
    return rrf_service(h);
}

int rrf_write_async(rrf_handle_t *h, const void *record, unsigned size, rrf_context_t *ctx)
{
    unsigned total_left, n;
    const void *readpos;

    if(!rrf_check_api_handle((const rrf_handle_t**)&h)) return 0;
    if(!rrf_arg_not_null(h, record, "record")) return 0;

    if(size == 0) return 1;

    if(ctx) {
        if(!rrf_arg_not_null(h, ctx->handle, "ctx.handle")) return 0;
        if(!rrf_arg_not_null(h, ctx->callback, "ctx.callback")) return 0;
    }

    if(!rrf_ensure_file_handles(h)) return 0;

    // write the header
    rrf_write_block_t *block = rrf_get_write_block(h, sizeof(unsigned), RRF_FIT);
    if(!block) {
        RRF_ERROR(h, RRF_WRITE_BUFFER_FULL, NULL);
        return 0;
    }
    unsigned *header = (unsigned*)(block->buffer + block->count);
    *header = size;
    block->count += sizeof(header);

    // write the record
    readpos = record;
    total_left = size;
    while(total_left > 0) {
        block = rrf_get_write_block(h, total_left, RRF_ANY);
        if(!block) {
            RRF_ERROR(h, RRF_WRITE_BUFFER_FULL, NULL);
            return 0;
        }

        void *writepos = block->buffer + block->count;
        n = RRF_MIN(RRF_WRITE_BLOCK_SIZE - block->count, total_left);

        memcpy(writepos, readpos, n);
        block->count += n;
        total_left -= n;
        readpos += n;
    }

    h->context = ctx;

    return 1;
}

int rrf_service(rrf_handle_t *h)
{
    rrf_write_block_t *block;

    if(!rrf_check_api_handle((const rrf_handle_t**)&h)) return 0;
    
    while(h->pending) {
        // todo: atomic
        block = h->pending;
        
        // write to the stream from an offset in the block buffer
        const void *writepos = block->buffer + block->written;
        unsigned count = block->count - block->written;;
        int written = fwrite(writepos, 1, count, h->current_file);
        if(written < 0) {
            RRF_ERROR(h, RRF_WRITE_FAILED, strerror(errno));
            return 0;
        }

        block->written += written;

        // notify callback of write complete
        if(h->context) {
            rrf_context_t *ctx = h->context;
            ctx->rvalue = writepos;
            ctx->size = written;
            ctx->callback(h, ctx);
        }

        // remove finished blocks from pending, place in the cache
        if(block->written == block->count) {
            h->pending = block->next;
            block->next = rrf_block_cache_head;
            rrf_block_cache_head = block;
        }
    
        h->write_index++;
        h->current_file_bytes += written;

        if(!rrf_roll_file_handles(h)) {
            break;
        }
    }

    return h->pending != NULL;
}

int rrf_error(rrf_handle_t *h, rrf_error_t *errors, unsigned count)
{
    if(count == 0) return 0;

    // return the low-level system error
    if(!h) {
        *errors = rrf_sys_error; 
        memset(&rrf_sys_error, 0, sizeof(rrf_sys_error));
        return 1;
    }

    if(h->errors == 0) {
        return 0;
    }

    count = RRF_MAX(count, h->errors);
    memcpy(errors, h->errorbuf, count * sizeof(rrf_error_t));
    h->errors -= count;

    return count;
}

