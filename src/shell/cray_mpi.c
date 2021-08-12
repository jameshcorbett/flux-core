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

#include <flux/hostlist.h>
#include <flux/shell.h>

#include "jansson.h"

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


static int safe_write(int fd, const void *buf, size_t size){
    ssize_t rc;
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
                                  int *task_cnts,
                                  int **tids)
{
    pals_pe_t *pes = NULL;
    int nodeidx, localidx, taskid;

    if (!(pes = calloc (ntasks, sizeof (pals_pe_t)))) {  // create one PE for each task
        return NULL;
    }
    for (nodeidx = 0; nodeidx < nnodes; nodeidx++) { // for each node identifier nodeidx ...
        for (localidx = 0; localidx < task_cnts[nodeidx]; localidx++) { // for each task within that node
            taskid = tids[nodeidx][localidx]; // get the global task ID of that task
            if (taskid >= ntasks) {
                shell_log_error ("taskid %d (on node %d) >= ntasks %d",
                                 taskid,
                                 nodeidx,
                                 ntasks);
                free (pes);
                return NULL;
            }
            pes[taskid].nodeidx = nodeidx;
            pes[taskid].localidx = localidx;
            pes[taskid].cmdidx = 0;
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
static int write_pals_nodes (int fd, json_t *nodelist_array)
{
    size_t index;
    int node_index = 0;
    json_t *value;
    struct hostlist *hlist;
    const char *entry;
    pals_node_t node;

    if (!(hlist = hostlist_create ())){
        return -1;
    }
    json_array_foreach(nodelist_array, index, value) {
        if (!(entry = json_string_value (value))
            || hostlist_append (hlist, entry) < 0){
            return -1;
        }
    }
    entry = hostlist_first (hlist);
    while (entry){
        node.nid = node_index++;
        if (snprintf (node.hostname, sizeof node.hostname, "%s", entry) >= sizeof node.hostname
            || safe_write (fd, &node, sizeof (pals_node_t)) < 0){
            return -1;
        }
        entry = hostlist_next (hlist);
    }
    return 0;
}


static int *get_task_counts (flux_shell_t *shell, int shell_size){
    int *task_counts;
    int i;

    if (!(task_counts = malloc(shell_size * sizeof shell_size))){
        return NULL;
    }
    for (i = 0; i < shell_size; ++i)
    {
        if (flux_shell_rank_info_unpack (shell, i, "{s:i}", "ntasks", &task_counts[i]) < 0){
            free (task_counts);
            return NULL;
        }
    }
    return task_counts;
}


static int **get_task_ids (int *task_counts, int shell_size){
    int **task_ids;
    int shell_rank, j;
    int curr_task_id = 0;

    if (!(task_ids = malloc(shell_size * sizeof task_counts))){
        return NULL;
    }
    for (shell_rank = 0; shell_rank < shell_size; ++shell_rank)
    {
        if(!(task_ids[shell_rank] = malloc(task_counts[shell_rank] * sizeof task_counts))){
            for (j = 0; j < shell_rank; ++j){
                free (task_ids[shell_rank]);
            }
            free (task_ids);
            return NULL;
        }
        for (j = 0; j < task_counts[shell_rank]; ++j)
        {
            task_ids[shell_rank][j] = curr_task_id++;
        }
    }
    return task_ids;
}



/*
 * Write the application information file
 */
static int create_apinfo (const char *apinfo_path, flux_shell_t *shell)
{
    int fd = -1, ret = 0, ntasks = 0;
    pals_header_t hdr;
    pals_cmd_t *cmds = NULL;
    pals_pe_t *pes = NULL;
    int shell_size, cores_per_task = 1;
    int *task_counts = NULL, **task_ids = NULL;
    json_t *nodelist_array;

    // Get relevant information from job

    if (flux_shell_info_unpack (shell, "{s:i, s:{s:{s:o}}}", "size", &shell_size, "R", "execution", "nodelist", &nodelist_array) < 0
        || !json_is_array (nodelist_array)
        || !(task_counts = get_task_counts (shell, shell_size))
        || !(task_ids = get_task_ids (task_counts, shell_size))){
        goto error;
    }
    for (int i = 0; i < shell_size; ++i)
    {
        ntasks += task_counts[i];
    }

    // if (nodelist == NULL) {
    //     shell_log_errno ("no nodelist found");
    //     goto error;
    // }

    // Get information to write
    build_header (&hdr, 1, ntasks, shell_size);
    if (!(pes = setup_pals_pes (ntasks, shell_size, task_counts, task_ids))
        || !(cmds = setup_pals_cmds (1, ntasks, shell_size, cores_per_task, pes))){
        goto error;
    }

    if ((fd = open (apinfo_path, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR)) == -1) {
        shell_log_errno ("Couldn't open apinfo file %s", apinfo_path);
        goto error;
    }

    // Write info
    if (safe_write (fd, &hdr, sizeof (pals_header_t)) < 0
        || safe_write (fd, cmds, (hdr.ncmds * sizeof (pals_cmd_t))) < 0
        || safe_write (fd, pes, (hdr.npes * sizeof (pals_pe_t))) < 0
        || write_pals_nodes (fd, nodelist_array) < 0){
        goto error;
    }

    // Flush changes to disk
    if (fsync (fd) == -1) {
        shell_log_errno ("Couldn't sync apinfo to disk");
        goto error;
    }

cleanup:

    if (task_counts)
        free (task_counts);
    if (task_ids){
        for (int i = 0; i < shell_size; ++i)
        {
            free (task_ids[i]);
        }
        free (task_ids);
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


static int set_environment_shell (flux_shell_t *shell, const char *apinfo_path){
    int rank = -1;
    json_int_t jobid;
    const char *tmpdir;

    if (flux_shell_info_unpack (shell, "{s:i, s:I}", "rank", &rank, "jobid", &jobid) < 0
        || flux_shell_setenvf (shell, 1, "PALS_NODEID", "%i", rank) < 0
        || flux_shell_setenvf (shell, 1, "PALS_APID", "%" JSON_INTEGER_FORMAT, jobid) < 0
        || !(tmpdir = flux_shell_getenv (shell, "FLUX_JOB_TMPDIR"))
        || flux_shell_setenvf (shell, 1, "PALS_SPOOL_DIR", "%s", tmpdir) < 0
        || flux_shell_setenvf (shell, 1, "PALS_APINFO", "%s", apinfo_path) < 0){
        return -1;
    }
    return 0;
}


static int cray_mpi_init (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    const char *tmpdir;
    char apinfo_path[1024];
    flux_shell_t *shell = flux_plugin_get_shell (p);

    if (!(tmpdir = flux_shell_getenv (shell, "FLUX_JOB_TMPDIR") )
        || snprintf (apinfo_path, sizeof (apinfo_path), "%s/%s", tmpdir, "libpals_apinfo") >= sizeof (apinfo_path)
        || create_apinfo(apinfo_path, shell) < 0
        || set_environment_shell (shell, apinfo_path) < 0){
        return -1;
    }
    return 0;
}


static int cray_mpi_task_init (flux_plugin_t *p,
                        const char *topic,
                        flux_plugin_arg_t *args,
                        void *data)
{
    flux_shell_t *shell = flux_plugin_get_shell (p);
    flux_shell_task_t *task;
    flux_cmd_t *cmd;
    int task_rank;

    if (!shell
        || !(task = flux_shell_current_task (shell))
        || !(cmd = flux_shell_task_cmd (task))
        || flux_shell_task_info_unpack (task, "{s:i}", "rank", &task_rank) < 0
        || flux_cmd_setenvf (cmd, 1, "PALS_RANKID", "%d", task_rank) < 0){
        return -1;
    }
    return 0;
}


struct shell_builtin builtin_cray_mpi = {
    .name = "cray_mpi",
    .init = cray_mpi_init,
    .task_init = cray_mpi_task_init,
};
