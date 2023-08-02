#include <stdlib.h>
#include <sys/queue.h>
#include <wiredtiger.h>
#include <bson.h>

#define DEFAULT_CONFIG "config_base=false,log=(enabled=true,path=journal,compressor=snappy)"

struct metadata {
    const char *ns;
    const char *ident;
    const char **idx_ident;
    size_t idx_cnt;

    void *md_data;
    size_t md_size;

    void *ss_data;
    size_t ss_size;
};

int open_session(const char *home, const char *config, WT_SESSION **sess);

int close_conn(WT_SESSION *sess);

int recover_to_stable(const char *home);

int get_metadata(WT_SESSION *sess, struct metadata ***rss, size_t *count);

void destroy_metadata(struct metadata *md);

int import_wt_ident(WT_SESSION *sess, const char *ident);

int import_catalog_ident(WT_SESSION *sess, const struct metadata *md);

int import_size_storer_ident(WT_SESSION *sess, const struct metadata *md);
