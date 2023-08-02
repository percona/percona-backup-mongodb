#include "wtl.h"

struct md_elem {
    struct metadata *rs;
    STAILQ_ENTRY(md_elem) _nav;
};
STAILQ_HEAD(md_head, md_elem);

int open_session(const char *home, const char *config, WT_SESSION **sess) {
    WT_CONNECTION *conn;
    int ret = wiredtiger_open(home, NULL, config, &conn);
    if (ret != 0) {
        return ret;
    }

    return conn->open_session(conn, NULL, NULL, sess);
}

int close_conn(WT_SESSION *sess) {
    return sess->connection->close(sess->connection, NULL);
}

int recover_to_stable(const char *home) {
    WT_SESSION *sess;
    int ret = open_session(home, DEFAULT_CONFIG, &sess);
    if (ret != 0) {
        return ret;
    }

    return close_conn(sess);
}

static struct metadata *metadata_from_bson(const bson_t *doc) {
    struct metadata *md = malloc(sizeof (struct metadata));
    if (md == NULL) {
        return NULL;
    }

    bson_iter_t iter;
    if (!bson_iter_init_find(&iter, doc, "ns")) {
        free(md);
        return NULL;
    }
    md->ns = bson_iter_dup_utf8(&iter, NULL);

    if (!bson_iter_init_find(&iter, doc, "ident")) {
        bson_free((void *)md->ns);
        free(md);
        return NULL;
    }
    md->ident = bson_iter_dup_utf8(&iter, NULL);
    md->idx_ident = NULL;
    md->idx_cnt = 0;

    bson_iter_t child;
    if (!bson_iter_init_find(&iter, doc, "idxIdent") || !bson_iter_recurse(&iter, &child)) {
        return md;
    }
    size_t idx = 0;
    while (bson_iter_next(&child)) {
        idx++;
    }
    if (idx == 0) {
        return md;
    }

    const char **idx_ident = malloc(idx * sizeof (char *));
    if (idx_ident == NULL) {
        return md;
    }

    (void)bson_iter_recurse(&iter, &child);
    for (idx = 0; bson_iter_next(&child); idx++) {
        idx_ident[idx] = bson_iter_dup_utf8(&child, NULL);
    }
    md->idx_cnt = idx;
    md->idx_ident = idx_ident;

    return md;
}

void destroy_metadata(struct metadata *md) {
    if (md->ns) {
        bson_free((void *)md->ns);
    }
    if (md->ident) {
        bson_free((void *)md->ident);
    }
    if (md->idx_ident) {
        while (md->idx_cnt--) {
            bson_free((void *)md->idx_ident[md->idx_cnt]);
        }
        free(md->idx_ident);
    }
    if (md->md_data) {
        free(md->md_data);
    }
    if (md->ss_data) {
        free(md->ss_data);
    }

    free(md);
}

static int get_size(WT_CURSOR *cur, const char *ident, void **data, size_t *size) {
    int ret;

    ret = cur->reset(cur);
    if (ret != 0) {
        return ret;
    }

    WT_ITEM key;
    while (!(ret = cur->next(cur))) {
        ret = cur->get_key(cur, &key);
        if (ret != 0) {
            break;
        }
        const char *n = (const char *)key.data;
        size_t c = strlen("table:");
        if (strncmp(n, "table:", c) != 0) {
            continue;
        }
        if (strncmp(n + c, ident, strlen(ident)) != 0) {
            continue;
        }

        WT_ITEM val;
        ret = cur->get_value(cur, &val);
        if (ret != 0) {
            break;
        }

        *data = memcpy(malloc(val.size), val.data, val.size);
        *size = val.size;
    }
    if (ret == WT_NOTFOUND) {
        ret = 0;
    }

    return ret;
}

int get_metadata(WT_SESSION *sess, struct metadata ***rss, size_t *count) {
    WT_CURSOR *md_cur;
    int ret = sess->open_cursor(sess, "table:_mdb_catalog", NULL, NULL, &md_cur);
    if (ret != 0) {
        return ret;
    }
    WT_CURSOR *ss_cur;
    ret = sess->open_cursor(sess, "table:sizeStorer", NULL, NULL, &ss_cur);
    if (ret != 0) {
        (void)md_cur->close(md_cur);
        return ret;
    }

    struct md_head head;
    STAILQ_INIT(&head);

    WT_ITEM val;
    while (true) {
        ret = md_cur->next(md_cur);
        if (ret != 0) {
            ret = 0;
            break;
        }
        (void)md_cur->get_value(md_cur, &val);
        bson_t *doc = bson_new_from_data(val.data, val.size);
        bson_iter_t iter;
        if (!bson_iter_init_find(&iter, doc, "ns")) {
            bson_destroy(doc);
            continue;
        }
        const char *ns = bson_iter_utf8(&iter, NULL);
        if (strcmp(ns, "db0.c0") != 0) {
            bson_destroy(doc);
            continue;
        }

        struct md_elem *elem = malloc(sizeof (struct md_elem));
        elem->rs = metadata_from_bson(doc);
        elem->rs->md_data = memcpy(malloc(val.size), val.data, val.size);
        elem->rs->md_size = val.size;
        bson_destroy(doc);
        if (elem->rs == NULL) {
            fprintf(stderr, "failed to parse md\n");
            free(elem);
            continue;
        }

        ret = get_size(ss_cur, elem->rs->ident, &elem->rs->ss_data, &elem->rs->ss_size);
        if (ret != 0) {
            fprintf(stderr, "get size: %s\n", sess->strerror(sess, ret));
            continue;
        }

        STAILQ_INSERT_TAIL(&head, elem, _nav);
        (*count)++;
    }

    *rss = malloc(*count * sizeof(struct metadata *));
    struct md_elem *elem = STAILQ_FIRST(&head);
    for (size_t i = 0; elem != NULL; i++) {
        (*rss)[i] = elem->rs;

        struct md_elem *temp = STAILQ_NEXT(elem, _nav);
        free(elem);
        elem = temp;
    }

    (void)ss_cur->close(ss_cur);
    (void)md_cur->close(md_cur);

    return ret;
}

int import_wt_ident(WT_SESSION *sess, const char *ident) {
    const char *template = "import=(enabled=true,repair=true,compare_timestamp=stable)";
    char *config = malloc(strlen(template) + strlen(ident) + strlen(ident));
    (void)sprintf(config, template, ident, ident);
    const char *tt = "table:%s";
    char *name = malloc(strlen(tt) + strlen(ident));
    (void)sprintf(name, tt, ident);

    int ret = sess->create(sess, name, config);
    free(name);
    free(config);
    return ret;
}

int import_catalog_ident(WT_SESSION *sess, const struct metadata *md) {
    WT_CURSOR *cur;
    int ret = sess->open_cursor(sess, "table:_mdb_catalog", NULL, NULL, &cur);
    if (ret != 0) {
        return ret;
    }
    ret = cur->largest_key(cur);
    if (ret != 0) {
        goto err;
    }
    int64_t key;
    ret = cur->get_key(cur, &key);
    if (ret != 0) {
        goto err;
    }
    WT_ITEM val = { md->md_data, md->md_size, NULL, 0, 0 };
    cur->set_key(cur, key + 1);
    cur->set_value(cur, val);
    ret = cur->insert(cur);

err:
    (void)cur->close(cur);
    return ret;
}

int import_size_storer_ident(WT_SESSION *sess, const struct metadata *md) {
    WT_CURSOR *cur;
    int ret = sess->open_cursor(sess, "table:sizeStorer", NULL, NULL, &cur);
    if (ret != 0) {
        return ret;
    }

    char *key = malloc(strlen("table:%s") + strlen(md->ident));
    sprintf(key, "table:%s", md->ident);
    WT_ITEM kk = { key, strlen(key), NULL, 0, 0 };
    WT_ITEM vv = { md->ss_data, md->ss_size, NULL, 0, 0 };
    cur->set_key(cur, kk);
    cur->set_value(cur, vv);
    ret = cur->insert(cur);

    free(key);
    (void)cur->close(cur);
    return ret;
}
