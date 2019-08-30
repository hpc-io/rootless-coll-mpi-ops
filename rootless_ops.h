/**
 * Rootless Operations for MPI (RLO)  Copyright (c) 2019, The
 * Regents of the University of California, through Lawrence Berkeley National
 * Laboratory (subject to receipt of any required approvals from the U.S.
 * Dept. of Energy). All rights reserved.
 *
 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Intellectual Property Office at
 * IPO@lbl.gov.
 *
 * NOTICE.  This Software was developed under funding from the U.S. Department
 * of Energy and the U.S. Government consequently retains certain rights.  As
 * such, the U.S. Government has been granted for itself and others acting on
 * its behalf a paid-up, nonexclusive, irrevocable, worldwide license in the
 * Software to reproduce, distribute copies to the public, prepare derivative
 * works, and perform publicly and display publicly, and to permit other to do so.
 *
 */


/*
 * rootless_ops.h
 *
 *  Created on: Jul 17, 2019
 *      Author: Tonglin Li
 */

#ifndef ROOTLESS_OPS_H_
#define ROOTLESS_OPS_H_

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>
#include <sys/types.h>



// ============= DEBUG GLOBALS =============
int total_pickup;
// ============= DEBUG GLOBALS =============

#define RLO_MSG_SIZE_MAX 32768
enum RLO_COMM_TAGS {//Used as MPI_TAG. Class 1
    RLO_BCAST, //class 1
    RLO_JOB_DONE,
    RLO_IAR_PROPOSAL, //class 2, under BCAST
    RLO_IAR_VOTE, // //class 2, under P2P
    RLO_IAR_DECISION, //class 2, under BCAST
    RLO_BC_TEARDOWN, //reserved
    RLO_IAR_TEARDOWN, //reserved
    RLO_P2P, //reserved
    RLO_SYS, //reserved
    RLO_ANY_TAG // == MPI_ANY_TAG
};

typedef enum REQ_STATUS {
    RLO_COMPLETED,//voted and decision made, either yes or no.
    RLO_IN_PROGRESS,
    RLO_FAILED,
    RLO_INVALID // default.
}RLO_Req_stat;

typedef int RLO_ID;
typedef int RLO_Vote;// used for & operation. 1 for yes, 0 for no.

typedef struct IAR_Single_Prop_CTX{
    void* my_proposal;
}ISP;

typedef int (*iar_cb_func_t)(const void *msg_buf, void *app_ctx);//const void *msg_buf, void *app_ctx

typedef struct progress_engine RLO_engine_t;

typedef struct RLO_msg_generic RLO_msg_t;
typedef struct Proposal_state RLO_proposal_state;
typedef unsigned long RLO_time_stamp;

typedef struct user_msg{
    char buf[RLO_MSG_SIZE_MAX + sizeof(int)];
    int type;
    RLO_ID pid;
    RLO_Vote vote;//0 = vote NO, 1 = vote yes, -1 = proposal, -2 = decision.
    RLO_time_stamp time_stamp;
    size_t data_len;
    char* data;
} RLO_user_msg;

struct RLO_msg_generic{
    //char buf[MSG_SIZE_MAX + sizeof(int)];// Make this always be the first field, so a pointer to it is the same as a pointer to the message struct
    RLO_user_msg msg_usr;
    char* data_buf; //= buf + sizeof(int), so data_buf size is MSG_SIZE_MAX
    int id_debug;
    /**
     * Only support BCAST, IAR_PROPOSAL for now, set this when the msg is in app pickup queue.
     */
    enum RLO_COMM_TAGS post_irecv_type;

    /**
     * Can only be BCAST, IAR_PROPOSAL(share the msg with decision)
     */
    enum RLO_COMM_TAGS send_type;

    /**
     * filled when repost irecv`
     */
    MPI_Request irecv_req;
    MPI_Status irecv_stat;
    /**
     * array of reqs for bcasts' isends
     */
    MPI_Request* bc_isend_reqs;
    MPI_Status* bc_isend_stats; //array of status for bcasts' isends
    /**
     * The state of associated proposal: completed/invalid/pending
     */
    RLO_proposal_state* prop_state;

    /**
     * Used internally for queue ops
     */
    RLO_msg_t *prev, *next;

    int send_cnt; //how many isend to monitor
    int ref_cnt;

    /**
     * If a message is picked up (and used or copied) by the user, if yes, it will be freed by user_msg_recycly().
     */
    int pickup_done;

    /**
     * Indicate if a message is completed on forwarding.
     */
    int fwd_done;

    /**
     * If this is a bc msg or s forward msg, used for MPI_Testall(num_isends, ...).
     * By default it's set to 0, and set to 1 for a new bc msg.
     */
    int bc_init;
};

/**
 * Wrap a generic message into a user message so to hide framework related information.
 */
RLO_user_msg* RLO_user_msg_new(RLO_msg_t* gen_msg_in);

/**
 *  Make a new generic type message.
 * @param eng: the progress engine used
 * @return a message pointer
 */
RLO_msg_t* RLO_msg_new_generic(RLO_engine_t* eng);


/**
 *  Prepare a message for bcast.
 * @param eng: the progress engine used
 * @param buf_in: the data buffer that will be bcasted.
 * @param send_size: send buffer size
 * @return a message pointer
 */
RLO_msg_t* RLO_msg_new_bc(RLO_engine_t* eng, void* buf_in, int send_size);
int RLO_msg_free(RLO_msg_t* msg_in);

/**
 * Check if a local message is done with it's communication job(bcast or forward).
 */
int RLO_msg_test_isends(RLO_engine_t* eng, RLO_msg_t* msg_in);

/**
 * Make a new progerss engine.
 * @param mpi_comm: a MPI communicator, such as MPI_COMM_WORLD
 * @param msg_size_max: maximal message size allowed in progress engine to process and transfer. Set it to be a big number to be safe, such as 32768.
 * @param approv_cb_func: user defined callback function, used in IAllReduce. Set it to NULL if not using IAllReduce.
 * @param app_ctx: user defined application context, used along with above callback function. Set it to NULL if not using IAllReduce.
 * @param app_proposal_action: user defined callback function, used in IAllReduce. Set it to NULL if not using IAllReduce.
 */
RLO_engine_t* RLO_progress_engine_new(MPI_Comm mpi_comm, size_t msg_size_max, void* approv_cb_func, void* app_ctx, void* app_proposal_action);

/**
 * Tear down an engine. It will free all resource used in eng.
 */
int RLO_progress_engine_cleanup(RLO_engine_t* eng);

/**
 * The core of the progress engine. It's called to turn the "gears" of the progress engine so to push it to next state.
 * @param eng: the progress engine used
 * @param msg_out: output message, only used to sample a message, for debugging purpose.
 */
int RLO_make_progress();

//make progreee with ONE engine
int RLO_make_progress_gen(RLO_engine_t* eng, RLO_msg_t** recv_msg_out);

int RLO_get_engine_id(RLO_engine_t* eng);
MPI_Comm RLO_get_my_comm(RLO_engine_t* eng);
/**
 * Rootless broadcast, can be initiated at any rank without predefine a "root" like the one in MPI_Bcast().
 * @param eng: the progress engine used
 * @param msg_in: the message to bcast. By default, the user should only use msg_new_bc() to make a new message.
 * @param tag: used to specify the message type. By default, the user should only use BCAST.
 * @see msg_new_bc()
 * @see msg_new_generic()
 */
int RLO_bcast_gen(RLO_engine_t* eng, RLO_msg_t* msg_in, enum RLO_COMM_TAGS tag);

/**
 * All received messages are picked up by this function, give one output at a time. User should keep calling it until return 0 so to get all messages in the mailbox.
 * Assuming the msg will be copied and stay safe, and will be unlinked from pickup_queue.
 * The user should free msg_out when it's done by calling user_msg_recycle().
 * @param eng: the progress engine used
 * @param msg_out: output parameter, gives the next available message in the mailbox.
 * @return 1 if there are still messages left, 0 if no more messages available.
 * @NOTE: if this function is called in a thread different from the progress_engine thread, there will be a thread safe issue.
 */
int RLO_user_pickup_next(RLO_engine_t* eng, RLO_user_msg** msg_out);
/**
 * A utility function to recycle the message and free resource.
 * User should always recycle a message after using it. Save the planet! (and a memory leakage.)
 */
int RLO_user_msg_recycle(RLO_engine_t* eng, RLO_user_msg* msg_in);


/* Submit a proposal, add it to waiting list, then return.
 * @return -1 if voting/decision making is not completed yet; 0 if proposal has been voted and declined, 1 if it's approved.
 */
int RLO_submit_proposal(RLO_engine_t* eng, char* proposal, size_t prop_size, RLO_ID my_proposal_id);
/*
 * Check if a proposal is done voting.
 */
int RLO_check_my_proposal_state(RLO_engine_t* eng, int pid);

/**
 * Get current status/voting results of my own proposal
 * @return -1 if not complete, 0 for being declined, 1 for being approved.
 */
int RLO_get_vote_my_proposal(RLO_engine_t* eng);

/**
 * Clear all fields in a proposal, including associated proposal message.
 */
int RLO_proposal_reset(RLO_proposal_state* ps);

int RLO_checkout_proposal(void* proposal_out);
// Utility functions
unsigned long RLO_get_time_usec();
void RLO_get_time_str(char *str_out);
int RLO_get_my_rank();
int RLO_get_world_size();

#endif /* ROOTLESS_OPS_H_ */
