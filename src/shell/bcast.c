/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#define FLUX_SHELL_PLUGIN_NAME "bcast"

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <unistd.h>
#include <jansson.h>
#include <flux/core.h>
#include <flux/shell.h>

#include "src/common/libutil/read_all.h"

#include "builtins.h"


static void bcast_cb (flux_t *h,
                            flux_msg_handler_t *mh,
                            const flux_msg_t *msg,
                            void *arg)
{
    struct shell_doom *doom = arg;
    json_t *task_info;

    assert (doom->shell->info->shell_rank == 0);

    if (doom->done)
        return;
    if (flux_request_unpack (msg, NULL, "o", &task_info) < 0) {
        shell_log_errno ("error parsing first task exit notification");
        return;
    }
    doom_post (doom, task_info);
    doom->done = true;
}


int flux_job_id_encode (flux_jobid_t id, const char *type,
                        char *buf, size_t bufsz);

static int bcast_init (flux_plugin_t *p,
                       const char *topic,
                       flux_plugin_arg_t *args,
                       void *data)
{

    int rank;
    flux_shell_t *shell = flux_plugin_get_shell (p);
    flux_future_t *svc;


    if (!shell
        || flux_shell_info_unpack (shell, "{s:i}", "rank", &rank) < 0){
        return -1;
    }
    if (rank == 0 && !(svc = flux_shell_service_register (shell, "bcast", bcast_callback, NULL) < 0)){
        return -1;
    }


    return 0;
}

struct shell_builtin builtin_batch = {
    .name = FLUX_SHELL_PLUGIN_NAME,
    .init = bcast_init,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
