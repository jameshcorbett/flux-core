/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>

#include <flux/shell.h>

#include "builtins.h"


/* Application file format version */
#define PALS_APINFO_VERSION 1

/* File header structure */
typedef struct {
    int version;
    size_t total_size;
    size_t comm_profile_size;
    size_t comm_profile_offset;
    int ncomm_profiles;
    size_t cmd_size;
    size_t cmd_offset;
    int ncmds;
    size_t pe_size;
    size_t pe_offset;
    int npes;
    size_t node_size;
    size_t node_offset;
    int nnodes;
    size_t nic_size;
    size_t nic_offset;
    int nnics;
} pals_header_t;

/* Network communication profile structure */
typedef struct {
    char tokenid[40];    /* Token UUID */
    int vni;             /* VNI associated with this token */
    int vlan;            /* VLAN associated with this token */
    int traffic_classes; /* Bitmap of allowed traffic classes */
} pals_comm_profile_t;

/* MPMD command information structure */
typedef struct {
    int npes;         /* Number of PEs in this command */
    int pes_per_node; /* Number of PEs per node */
    int cpus_per_pe;  /* Number of CPUs per PE */
} pals_cmd_t;

/* PE (i.e. task) information structure */
typedef struct {
    int localidx; /* Node-local PE index */
    int cmdidx;   /* Command index for this PE */
    int nodeidx;  /* Node index this PE is running on */
} pals_pe_t;

/* Node information structure */
typedef struct {
    int nid;           /* Node ID */
    char hostname[64]; /* Node hostname */
} pals_node_t;

/* NIC address type */
typedef enum { PALS_ADDR_IPV4, PALS_ADDR_IPV6, PALS_ADDR_MAC } pals_address_type_t;

/* NIC information structure */
typedef struct {
    int nodeidx;                      /* Node index this NIC belongs to */
    pals_address_type_t address_type; /* Address type for this NIC */
    char address[40];                 /* Address of this NIC */
} pals_nic_t;


static int safe_write(int fd, char *buf, size_t size){
    int rc;
    while(size > 0) {
        rc = write(fd, buf, size);
            if (rc < 0) {
            if ((errno == EAGAIN) || (errno == EINTR))
                continue;
            return -1;
        } else {
            buf += rc;
            size -= rc;
        }
    }
    return 0;
}


/*
 * Return an array of pals_pe_t structures.
 */
static pals_pe_t *setup_pals_pes (int ntasks,
                                  int nnodes,
                                  uint16_t *task_cnts,
                                  uint32_t **tids,
                                  uint32_t *tid_offsets)
{
    pals_pe_t *pes = NULL;
    int nodeidx, localidx, taskid;

    if (!(pes = calloc (ntasks, sizeof (pals_pe_t)))) {
        return NULL;
    }
    for (nodeidx = 0; nodeidx < nnodes; nodeidx++) {
        for (localidx = 0; localidx < task_cnts[nodeidx]; localidx++) {
            taskid = tids[nodeidx][localidx];
            if (taskid >= ntasks) {
                shell_log_errno ("task %d node %d >= ntasks %d; skipping",
                                 taskid,
                                 nodeidx,
                                 ntasks);
                continue;
            }
            pes[taskid].nodeidx = nodeidx;
            pes[taskid].localidx = localidx;

            if (!tid_offsets) {
                pes[taskid].cmdidx = 0;
            } else {
                pes[taskid].cmdidx = tid_offsets[taskid];
            }
        }
    }
    return pes;
}

/*
 * Return an array of pals_cmd_t structures.
 */
static pals_cmd_t *setup_pals_cmds (int ncmds,
                                    int ntasks,
                                    int nnodes,
                                    int cpus_per_task,
                                    pals_pe_t *pes)
{
    pals_cmd_t *cmds;
    int peidx, cmdidx, nodeidx, max_ppn;
    int **cmd_ppn;

    // Allocate and initialize arrays
    if (!(cmds = calloc (ncmds, sizeof (pals_cmd_t)))) {
        return NULL;
    }
    if (!(cmd_ppn = calloc (ncmds, sizeof (int *)))) {
        free (cmds);
        return NULL;
    }
    for (cmdidx = 0; cmdidx < ncmds; cmdidx++) {
        if (!(cmd_ppn[cmdidx] = calloc (nnodes, sizeof (int)))) {
            for (int i = 0; i < cmdidx; i++) {
                free (cmd_ppn[i]);
            }
            free (cmd_ppn);
            free (cmds);
            return NULL;
        }
    }

    // Count number of PEs for each command/node
    for (peidx = 0; peidx < ntasks; peidx++) {
        cmdidx = pes[peidx].cmdidx;
        nodeidx = pes[peidx].nodeidx;
        if (cmdidx >= 0 && cmdidx < ncmds && nodeidx >= 0 && nodeidx < nnodes) {
            cmd_ppn[cmdidx][nodeidx]++;
        }
    }

    // Fill in command information
    for (cmdidx = 0; cmdidx < ncmds; cmdidx++) {
        // NOTE: we don't know each job's depth for a heterogeneous job
        cmds[cmdidx].cpus_per_pe = cpus_per_task;

        // Find the total PEs and max PEs/node for this command
        max_ppn = 0;
        for (nodeidx = 0; nodeidx < nnodes; nodeidx++) {
            cmds[cmdidx].npes += cmd_ppn[cmdidx][nodeidx];
            if (cmd_ppn[cmdidx][nodeidx] > max_ppn) {
                max_ppn = cmd_ppn[cmdidx][nodeidx];
            }
        }
        free (cmd_ppn[cmdidx]);

        cmds[cmdidx].pes_per_node = max_ppn;
    }

    free (cmd_ppn);
    return cmds;
}

/*
 * Fill in the apinfo header
 */
static void build_header (pals_header_t *hdr, int ncmds, int npes, int nnodes)
{
    size_t offset = sizeof (pals_header_t);

    memset (hdr, 0, sizeof (pals_header_t));
    hdr->version = PALS_APINFO_VERSION;

    hdr->comm_profile_size = sizeof (pals_comm_profile_t);
    hdr->comm_profile_offset = offset;
    hdr->ncomm_profiles = 0;
    offset += hdr->comm_profile_size * hdr->ncomm_profiles;

    hdr->cmd_size = sizeof (pals_cmd_t);
    hdr->cmd_offset = offset;
    hdr->ncmds = ncmds;
    offset += hdr->cmd_size * hdr->ncmds;

    hdr->pe_size = sizeof (pals_pe_t);
    hdr->pe_offset = offset;
    hdr->npes = npes;
    offset += hdr->pe_size * hdr->npes;

    hdr->node_size = sizeof (pals_node_t);
    hdr->node_offset = offset;
    hdr->nnodes = nnodes;
    offset += hdr->node_size * hdr->nnodes;

    hdr->nic_size = sizeof (pals_nic_t);
    hdr->nic_offset = offset;
    hdr->nnics = 0;
    offset += hdr->nic_size * hdr->nnics;

    hdr->total_size = offset;
}

/*
 * Write the job's node list to the file
 */
static int write_pals_nodes (int fd)
{
    pals_node_t node;
    char host[sizeof (node.hostname)];

    gethostname(host, sizeof(host));  // HACK
    memset (&node, 0, sizeof (pals_node_t));
    if (1) {  // HACK
        snprintf (node.hostname, sizeof (node.hostname), "%s", host);
        node.nid = 0;
        if (safe_write (fd, (char *) &node, sizeof (pals_node_t)) < 0){
            return -1;
        }
    }
    return 0;
}

/*
 * Write the application information file
 */
static int create_apinfo (void)
{
    int fd = -1;
    int ret = 0;
    pals_header_t hdr;
    pals_cmd_t *cmds = NULL;
    pals_pe_t *pes = NULL;
    int ntasks, ncmds, nnodes, cores_per_task;
    uint16_t task_cnts[] = {0, 1};
    uint32_t tids[] = {0, 1};
    uint32_t *tid_offsets = NULL;
    bool free_tid_offsets = 0;
    uint32_t *tid_ptr = tids;

    // Get relevant information from job

    ntasks = 1;
    nnodes = 1;
    ncmds = 1;
    cores_per_task = 1;
    //tids = job->msg->global_task_ids;
    //nodelist = job->msg->complete_nodelist;


    // Make sure we've got everything
    // if (ntasks <= 0) {
    //     shell_log_errno ("no tasks found");
    //     goto error;
    // }
    // if (ncmds <= 0) {
    //     shell_log_errno ("no cmds found");
    //     goto error;
    // }
    // if (nnodes <= 0) {
    //     shell_log_errno ("no nodes found");
    //     goto error;
    // }
    // if (&task_cnts == NULL) {
    //     shell_log_errno ("no per-node task counts");
    //     goto error;
    // }
    // if (&tids == NULL) {
    //     shell_log_errno ("no task IDs found");
    //     goto error;
    // }
    // if (nodelist == NULL) {
    //     shell_log_errno ("no nodelist found");
    //     goto error;
    // }

    // Get information to write
    build_header (&hdr, ncmds, ntasks, nnodes);
    if (!(pes = setup_pals_pes (ntasks, nnodes, task_cnts, &tid_ptr, tid_offsets))) {
        goto error;
    }
    if (!(cmds = setup_pals_cmds (ncmds, ntasks, nnodes, cores_per_task, pes))) {
        goto error;
    }

    // Create the file
    if ((fd = open ("apinfo", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR)) == -1) {
        shell_log_errno ("Couldn't open apinfo file");
        goto error;
    }

    // Write info
    if (safe_write (fd, (char *) &hdr, sizeof (pals_header_t)) < 0
        || safe_write (fd, (char *) cmds, (hdr.ncmds * sizeof (pals_cmd_t))) < 0
        || safe_write (fd, (char *) pes, (hdr.npes * sizeof (pals_pe_t)) < 0)
        || write_pals_nodes (fd) < 0){
        goto error;
    }

    // Flush changes to disk
    if (fsync (fd) == -1) {
        shell_log_errno ("Couldn't sync apinfo to disk");
        goto error;
    }

cleanup:
    if (free_tid_offsets && tid_offsets){
        free (tid_offsets);
    }

    if (pes)
        free (pes);
    if (cmds)
        free (cmds);
    close (fd);
    return ret;
error:
    ret = -1;
    goto cleanup;
}


static int shell_rank (flux_shell_t *shell)
{
    int rank = -1;
    if (flux_shell_info_unpack (shell, "{s:i}", "rank", &rank) < 0)
        return -1;
    return rank;
}


static int cray_mpi_init (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    flux_shell_t *shell = flux_plugin_get_shell (p);
    if (shell_rank (shell) == 0){
        return create_apinfo();
    }
    return 0;
}


struct shell_builtin builtin_cray_mpi = {
    .name = "cray_mpi",
    .init = cray_mpi_init,
};
