/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>
#include <errno.h>
#include <unistd.h>

#include "job.h"



static void read_from_file(){

	char* buf;
	ssize_t bytes_read;

	if (!buf = malloc (chunksize * sizeof (*buf)))
		goto error;
	while (1){
		errno = 0;
		if ((bytes_read = read (fd, buf, chunksize)) == 0){
			if (errno == EINTR){
				continue;
			}
		}
		break;
	}



error:
}


flux_future_t *flux_job_file_bcast (flux_t *h, flux_jobid_t id, const char *path, const char *destpath, size_t chunksize, int flags){

	int fd;
	char topicbuf[512] = "bcast.";
	size_t topicbuf_len = strlen(topicbuf);
	flux_future_t *fut;

	if (!path || !destpath || chunksize < 1 || !h){
		errno = EINVAL;
		return NULL;
	}
	if (!(fd = open(path, O_RDONLY))){
		errno = ENOENT;
		return NULL;
	}
	if (flux_job_id_encode(id, "dec", &topicbuf[topicbuf_len], sizeof (topicbuf) - topicbuf_len) < 0
		|| !(fut = flux_rpc_pack (h, topicbuf, FLUX_NODEID_ANY,
                              0, "{s:s}", "destination", destpath)))
		return -1;



error:
	close(fd);
	return NULL;
}
