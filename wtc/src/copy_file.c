#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <copyfile.h>

static int copy_file(const char *from, const char *to, const char *filename) {
    size_t base = strlen(filename) + 2;
    char *src = malloc(strlen(from) + base);
    if (src == NULL) {
        return -1;
    }
    char *dst = malloc(strlen(to) + base);
    if (dst == NULL) {
        free(src);
        return -1;
    }

    (void)sprintf(src, "%s/%s", from, filename);
    (void)sprintf(dst, "%s/%s", to, filename);

    int ret = copyfile(src, dst, NULL, COPYFILE_ALL);

    free(dst);
    free(src);

    return ret;
}
