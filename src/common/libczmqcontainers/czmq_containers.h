/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#ifndef _CZMQ_CONTAINERS_H
#define _CZMQ_CONTAINERS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <czmq.h>
#include "czmq_rename.h"

typedef struct _zhashx_t zhashx_t;

#ifndef CZMQ_EXPORT
#define CZMQ_EXPORT
#endif

#include "zhashx.h"

#ifdef __cplusplus
}
#endif

#endif
