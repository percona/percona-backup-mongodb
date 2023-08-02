#include "wtl.h"
#include "copy_file.c"

#define SRC_HOME "src"
#define DST_HOME "dst"

int main(void) {
    char *tmp_dir = mkdtemp(strdup("wtc.XXXXXX"));
    if (tmp_dir == NULL) {
        perror("mkdtemp");
        return EXIT_FAILURE;
    }
    char t[1024];
    (void)sprintf(t, "%s/journal", tmp_dir);
    if (mkdir(t, 0777) != 0) {
        perror("mkdir journal");
        return EXIT_FAILURE;
    }
    if (copy_file(SRC_HOME, tmp_dir, "WiredTiger") != 0) {
        perror("copy WiredTiger");
        return EXIT_FAILURE;
    }
    if (copy_file(SRC_HOME, tmp_dir, "WiredTiger.backup") != 0) {
        perror("copy WiredTiger.backup");
        return EXIT_FAILURE;
    }
    if (copy_file(SRC_HOME, tmp_dir, "WiredTigerHS.wt") != 0) {
        perror("copy WiredTigerHS.wt");
        return EXIT_FAILURE;
    }

    int ret = recover_to_stable(tmp_dir);
    if (ret != 0) {
        fprintf(stderr, "error: %s\n", wiredtiger_strerror(ret));
        return EXIT_FAILURE;
    }

    if (copy_file(SRC_HOME, tmp_dir, "_mdb_catalog.wt") != 0) {
        perror("copy _mdb_catalog.wt");
        return EXIT_FAILURE;
    }
    if (copy_file(SRC_HOME, tmp_dir, "sizeStorer.wt") != 0) {
        perror("copy _mdb_catalog.wt");
        return EXIT_FAILURE;
    }

    WT_SESSION *sess;
    ret = open_session(tmp_dir, DEFAULT_CONFIG ",readonly", &sess);
    if (ret != 0) {
        fprintf(stderr, "error: %s\n", wiredtiger_strerror(ret));
        return EXIT_FAILURE;
    }

    struct metadata **mds = NULL;
    size_t count = 0;
    ret = get_metadata(sess, &mds, &count);
    if (ret != 0) {
        fprintf(stderr, "error: %s\n", wiredtiger_strerror(ret));
        return EXIT_FAILURE;
    }

    ret = close_conn(sess);
    if (ret != 0) {
        fprintf(stderr, "error: %s\n", wiredtiger_strerror(ret));
    }
    free(tmp_dir);

    ret = open_session(DST_HOME, DEFAULT_CONFIG, &sess);
    if (ret != 0) {
        fprintf(stderr, "error: %s\n", wiredtiger_strerror(ret));
        return EXIT_FAILURE;
    }
    for (size_t n = 0; n != count; n++) {
        struct metadata *md = mds[n];
        char *filename = malloc(strlen(md->ident) + 4);
        (void)sprintf(filename,"%s.wt", md->ident);
        ret = copy_file(SRC_HOME, DST_HOME, filename);
        if (ret != 0) {
            fprintf(stderr, "copy %s: %s\n", filename, strerror(ret));
        }
        free(filename);

        ret = import_wt_ident(sess, md->ident);
        if (ret != 0) {
            fprintf(stderr, "import_ident(%s): %s\n",
                md->ident, sess->strerror(sess, ret));
        }

        for (size_t f = 0; f != md->idx_cnt; f++) {
            char *filename = malloc(strlen(md->idx_ident[f]) + 4);
            (void)sprintf(filename,"%s.wt", md->idx_ident[f]);
            if (copy_file(SRC_HOME, DST_HOME, filename) != 0) {
                perror("copy md->ident");
            }
            free(filename);

            ret = import_wt_ident(sess, md->idx_ident[f]);
            if (ret != 0) {
                fprintf(stderr, "import_ident(%s): %s\n",
                    md->idx_ident[f], sess->strerror(sess, ret));
            }
        }

        ret = import_catalog_ident(sess, md);
        if (ret != 0) {
            fprintf(stderr, "import_ident(%s): %s\n",
                md->ident, sess->strerror(sess, ret));
        }
        ret = import_size_storer_ident(sess, md);
        if (ret != 0) {
            fprintf(stderr, "import_ident(%s): %s\n",
                md->ident, sess->strerror(sess, ret));
        }

        destroy_metadata(md);
    }
    free(mds);

    (void)close_conn(sess);
    return EXIT_SUCCESS;
}
