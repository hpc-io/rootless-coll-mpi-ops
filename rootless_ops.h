/*
 * rootless_ops.h
 *
 *  Created on: Jul 17, 2019
 *      Author: Tonglin Li
 */

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

#ifndef ROOTLESS_OPS_H_
#define ROOTLESS_OPS_H_

// ============= DEBUG GLOBALS =============
int total_pickup;
// ============= DEBUG GLOBALS =============

#define MSG_SIZE_MAX 32768
enum COM_TAGS {//Used as MPI_TAG. Class 1
    BCAST, //class 1
    JOB_DONE,
    IAR_PROPOSAL, //class 2, under BCAST
    IAR_VOTE, // //class 2, under P2P
    IAR_DECISION, //class 2, under BCAST
    BC_TEARDOWN, //class 2, under SYS, for teardowon use
    IAR_TEARDOWN, //class 2, under SYS, for teardowon use
    P2P, //class 1
    SYS, //class 1
    ANY_TAG // == MPI_ANY_TAG
};

typedef enum REQ_STATUS {
    COMPLETED,
    IN_PROGRESS,
    FAILED,
    INVALID // default.
}Req_stat;

unsigned long get_time_usec();
void get_time_str(char *str_out);


typedef int ID;
typedef int Vote;// used for & operation. 1 for yes, 0 for no.

typedef struct IAR_Single_Prop_CTX{
    void* my_proposal;
}ISP;

typedef int (*iar_cb_func_t)(const void *msg_buf, void *app_ctx);//const void *msg_buf, void *app_ctx


// Message package protocol and functions
typedef struct Proposal_buf{
    ID pid;
    Vote vote;//0 = vote NO, 1 = vote yes, -1 = proposal, -2 = decision.
    size_t data_len;
    char* data;
}PBuf;
int pbuf_serialize(ID pid_in, Vote vote, size_t data_len_in, char* data_in, char* buf_out, size_t* buf_len_out);
void pbuf_free(PBuf* pbuf);
int pbuf_deserialize(char* buf_in, PBuf* pbuf_out);

typedef struct progress_engine engine_t;

typedef struct msg_generic msg_t;
typedef struct Proposal_state proposal_state;

typedef struct user_msg{
    char buf[MSG_SIZE_MAX + sizeof(int)];
    int type;
    ID pid;
    Vote vote;//0 = vote NO, 1 = vote yes, -1 = proposal, -2 = decision.
    size_t data_len;
    char* data;
} user_msg;

struct msg_generic{
    //char buf[MSG_SIZE_MAX + sizeof(int)];// Make this always be the first field, so a pointer to it is the same as a pointer to the message struct
    user_msg msg_usr;
    char* data_buf; //= buf + sizeof(int), so data_buf size is MSG_SIZE_MAX
    int id_debug;

    enum COM_TAGS post_irecv_type;//Only support BCAST, IAR_PROPOSAL for now, set this when the msg is in app pickup queue.
    enum COM_TAGS send_type; //BCAST, IAR_PROPOSAL(share the msg  with decision)
    MPI_Request irecv_req; //filled when repost irecv
    MPI_Status irecv_stat;
    MPI_Request* bc_isend_reqs; //array of reqs for bcasts' isends
    MPI_Status* bc_isend_stats; //array of status for bcasts' isends

    proposal_state* prop_state;

    msg_t *prev, *next; //for generic queue ops

    int send_cnt; //how many isend to monitor
    int ref_cnt;
    int pickup_done; //user mark pickup is done.
    int fwd_done; // system mark forward is done.

    int bc_init;
        //if this is a bc msg or s forward msg, used for MPI_Testall(num_isends, ...)
        //By default it's set to 0, and set to 1 for a new bc msg.
};

user_msg* user_msg_new(msg_t* gen_msg_in);

/**
 *  Make a new generic type message.
 * @param eng: the progress engine used
 * @return a message pointer
 */
msg_t* msg_new_generic(engine_t* eng);
msg_t* msg_new_bc(engine_t* eng, void* buf_in, int send_size);
int msg_free(msg_t* msg_in);

int msg_test_isends(engine_t* eng, msg_t* msg_in);

/**
 * Make a new progerss engine.
 * @param mpi_comm: a MPI communicator, such as MPI_COMM_WORLD
 * @param msg_size_max: maximal message size allowed in progress engine to process and transfer. Set it to be a big number to be safe, such as 32768.
 * @param approv_cb_func: user defined callback function, used in IAllReduce. Set it to NULL if not using IAllReduce.
 * @param app_ctx: user defined application context, used along with above callback function. Set it to NULL if not using IAllReduce.
 * @param app_proposal_action: user defined callback function, used in IAllReduce. Set it to NULL if not using IAllReduce.
 */
engine_t* progress_engine_new(MPI_Comm mpi_comm, size_t msg_size_max, void* approv_cb_func, void* app_ctx, void* app_proposal_action);

/**
 * The core of the progress engine. It's called to turn the "gears" of the progress engine so to push it to next state.
 * @param eng: the progress engine used
 * @param msg_out: output message, only used to sample a message, for debugging purpose.
 */
int make_progress_gen(engine_t* eng, msg_t** msg_out);

/**
 * Rootless broadcast, can be initiated at any rank without predefine a "root" like the one in MPI_Bcast().
 * @param eng: the progress engine used
 * @param msg_in: the message to bcast. By default, the user should only use msg_new_bc() to make a new message.
 * @param tag: used to specify the message type. By default, the user should only use BCAST.
 * @see msg_new_bc()
 * @see msg_new_generic()
 */
int bcast_gen(engine_t* eng, msg_t* msg_in, enum COM_TAGS tag);

/**
 * All received messages are picked up by this function, give one output at a time. User should keep calling it until return 0 so to get all messages in the mailbox.
 * @param eng: the progress engine used
 * @param msg_out: output parameter, gives the next available message in the mailbox.
 * @return 1 if there are still messages left, 0 if no more messages available.
 */
int user_pickup_next(engine_t* eng, user_msg** msg_out);
/**
 * A utility function to recycle the message and free resource.
 * User should always recycle a message after using it. Save the planet! (and a memory leakage.)
 */
int user_msg_recycle(engine_t* eng, user_msg* msg_in);

/**
 * Tear down an engine. It will free all resource used in eng.
 */
int engine_cleanup(engine_t* eng);

// Submit a proposal, add it to waiting list, then return.
int iar_submit_proposal(engine_t* eng, char* proposal, size_t prop_size, ID my_proposal_id);
int check_proposal_state(engine_t* eng, int pid);
int get_vote_my_proposal(engine_t* eng);



int proposal_reset(proposal_state* ps);
int get_my_rank();
int get_world_size();
#endif /* ROOTLESS_OPS_H_ */
