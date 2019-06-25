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
//#include "progress_engine.h"


// ============= DEBUG GLOBALS =============
int total_pickup;
// ============= DEBUG GLOBALS =============

#define MSG_SIZE_MAX 32768
#define PROPOSAL_POOL_SIZE 16 //maximal concurrent proposal supported
#define ISEND_CONCURRENT_MAX 128 //maximal number of concurrent and unfinished isend, used to set MPI_Request and MPI_State arrays for MPI_Waitall().
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

enum MSG_TAGS {//Used as a msg tag, it's a field of the msg. Class 2
    IAR_Vote // to replace IAR_VOTE in mpi_tag
};
typedef struct bcomm_generic_msg bcomm_GEN_msg_t;
typedef int ID;
typedef int Vote;// used for & operation. 1 for yes, 0 for no.
typedef struct Proposal_state{
    ID pid; // proposal ID, default = -1;
    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
//    int proposal_sent_cnt;
    Vote vote; //accumulated vote, default = 1;
    int votes_needed; //num of votes needed to report to parent, equals to the number of sends of a proposal
    int votes_recved;
    bcomm_GEN_msg_t* proposal_msg;//the last place holds a proposal, should be freed when decision is made and executed.
} proposal_state; //clear when vote result is reported.

typedef struct queue {
    bcomm_GEN_msg_t* head;
    bcomm_GEN_msg_t* tail;
    int msg_cnt;
}queue;

int queue_append(queue* q, bcomm_GEN_msg_t* msg);
int queue_remove(queue* q, bcomm_GEN_msg_t* msg);

typedef enum REQ_STATUS {
    COMPLETED,
    IN_PROGRESS,
    FAILED
}REQ_STATUS;

typedef struct bcomm_token_t {
    ID req_id;
    REQ_STATUS req_stat;
    bool completed;
    bool vote_result;
    struct bcomm_token_t* next;
} bcomm_token_t;

typedef struct BCastCommunicator {
    /* MPI fields */
    MPI_Comm my_comm;                   /* MPI communicator to use */
    int my_rank;                        /* Local rank value */
    int world_size;                     /* # of ranks in communicator */

    /* Message fields */
    size_t msg_size_max;                /* Maximum message size */
    void *user_send_buf;                /* Buffer for user to send messages in */
    void *sys_send_buf;                 /* For internal message sends, simmilar as user_send_buf */
    /* Skip ring fields */
    int my_level;                       /* Level of rank in ring */
    int last_wall;                      /* The closest rank that has higher level than rank world_size - 1 */
    int world_is_power_of_2;            /* Whether the world size is a power of 2 */

    /* Send fields */
    int send_channel_cnt;               /* # of outgoing channels from this rank */
    int send_list_len;                  /* # of outgoing ranks to send to */
    int* send_list;                     /* Array of outgoing ranks to send to */
    void *send_buf;                     /* Buffer for sending messages */
    void *send_buf_internal;                     /* Buffer for sending messages */
    int fwd_send_cnt[2];                /* # of outstanding non-blocking forwarding sends for each receive buffer */
    MPI_Request* fwd_isend_reqs[2];     /* Array for non-blocking forwarding send requests for each receive buffer */
    MPI_Status* fwd_isend_stats[2];     /* Array for non-blocking forwarding send statuses for each receive buffer */
    int bcast_send_cnt;                 /* # of outstanding non-blocking broadcast sends */
    MPI_Request* bcast_isend_reqs;      /* Array for non-blocking broadcast send requests */
    MPI_Status* bcast_isend_stats;      /* Array for non-blocking broadcast send statuses */

    /* Receive fields */
    MPI_Request irecv_req;              /* Request for incoming messages */
    unsigned curr_recv_buf_index;             /* Current buffer for receive */
    char* recv_buf[2];                  /* Buffers for incoming messages */
    
    /* I_all_reduce fields*/
    bool IAR_active;                    /* Set true if it's doing IAR, and set false after IAR is done.*/
    char* IAR_recv_buf;                 /* IallReduce recv buf */
    Vote vote_my_proposal_no_use;          /* Used only by an proposal-active rank. 1 for agree, 0 for decline. Accumulate votes for a proposal that I just submitted. */
    proposal_state my_own_proposal;      /* Set only when I'm a IAR starter, maintain status for my own proposal */
    proposal_state proposal_state_pool[PROPOSAL_POOL_SIZE];        /* To support multiple proposals, use a vote pool for each proposal. Use linked list if concurrent proposal number is large. */
    char* my_proposal;                  /* This is used to compare against received proposal, should be updated timely */
    char* send_buf_my_vote;
    int recv_vote_cnt;
//    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
//    int proposal_sent_cnt;              /* How many children received this proposal, it's the number of votes expected. */

    /* Operation counters */
    int my_bcast_cnt;
    int bcast_recv_cnt;
    /* Request progress status*/
    bcomm_token_t* req_stat;
} bcomm;

bcomm *bcomm_init(MPI_Comm comm, size_t msg_size_max);

char DEBUG_MODE = 'O';
typedef struct {
    FILE* log_file;
    int my_rank;
} Log;
Log MY_LOG;


// Message package protocol and functions
typedef struct Proposal_buf{
    ID pid;
    Vote vote;//0 = vote NO, 1 = vote yes, -1 = proposal, -2 = decision.
    unsigned int data_len;
    char* data;
}PBuf;
int pbuf_serialize(ID pid_in, Vote vote, unsigned int data_len_in, char* data_in, char* buf_out, unsigned int* buf_len_out);
void pbuf_free(PBuf* pbuf);
int pbuf_deserialize(char* buf_in, PBuf* pbuf_out);


// Skip ring utility funcitons
int get_origin(void* buf_in);
int check_passed_origin(const bcomm* my_bcomm, int origin_rank, int to_rank);




unsigned long get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return 1000000 * tv.tv_sec + tv.tv_usec;
}

void get_time_str(char *str_out) {
    time_t rawtime;
    struct tm * timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    sprintf(str_out, "%d:%d:%d", timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}

//Proposal pool ops
int proposalPool_init(proposal_state* pp_in_out, bcomm_GEN_msg_t* prop_msg_in);
int proposalPool_proposal_add(proposal_state* pools, proposal_state* pp_in);
int proposalPool_proposal_setNeededVoteCnt(proposal_state* pools, ID k, int cnt);
int proposalPool_vote_merge(proposal_state* pools, ID k, Vote v, int* p_index_out);
int proposalPool_get(proposal_state* pools, ID k, proposal_state* result);
int proposalPool_get_index(proposal_state* pools, ID k);
int proposalPool_rm(proposal_state* pools, ID k);
int proposalPools_reset(proposal_state* pools);


//Old version of operations
int bcast(bcomm* my_bcomm, enum COM_TAGS tag);


/* ----------------------------------------------------------------------- */
/* ----------------- refactoring for progress engine BEGIN --------------- */

enum State_BC {
    NEW, // I_recv not completed yet
    NOT_APP_RECV_STILL_FORWARDING, // need appl to look at buffer, still forwarding to children
    APP_RECV_STILL_FORWARDING,  //app has recevied, still forwarding to children
    NOT_APP_RECV_DONE_FORWARDING, // need appl to look at buffer, done forwardiing to children
    APP_RECV_DONE_FORWARDING,  //app has recevied, done forwarding to children
    APP_RECV_AND_FINISHED_STILL_FORWARDING,
    // appl finished looking at buffer, still forwarding buffer to children
    APP_RECV_AND_FINISHED_DONE_FORWARDING
    // app finished with buffer, done forwadin to children
};

// IAR is built on top of bcast, so it's not aware of BC related state changes, only cares about proposal state change.
enum State_IAR {
    //Proposal processing states
    NEW_START_WAIT_VOTE, //the rank started a new proposal, waiting for votes
    PASSIVE_RECV_APPROVED, // passive leaf rank recved a new proposal and approved, voting yes
    PASSIVE_RECV_WAIT_VOTE, // passive non-leaf rank received a new proposal, and approved locally, in forwarding and will wait for children votes
    PASSIVE_RECV_DECLINED, // passive newly received proposal get declined, and is voting no
    ACTIVE_RECV_I_WIN,  //active rank received a conflicting proposal, and (local) wins the compete(), voting no
    ACTIVE_RECV_I_LOSE, //lose compete, voting yes
};


struct bcomm_generic_msg{
    char buf[MSG_SIZE_MAX + sizeof(int)];// Make this always be the first field, so a pointer to it is the same as a pointer to the message struct
    char* data_buf; //= buf + sizeof(int), so data_buf size is MSG_SIZE_MAX
    int id_debug;

    enum COM_TAGS post_irecv_type;//Only support BCAST, IAR_PROPOSAL for now, set this when the msg is in app pickup queue.
    MPI_Request irecv_req; //filled when repost irecv
    MPI_Status irecv_stat;
    MPI_Request* bc_isend_reqs; //array of reqs for bcasts' isends
    MPI_Status* bc_isend_stats; //array of status for bcasts' isends

    proposal_state* prop_state;

    bcomm_GEN_msg_t *prev, *next; //for generic queue ops

    int send_cnt; //how many isend to monitor
    int ref_cnt;
    int pickup_done; //user mark pickup is done.
    int fwd_done; // system mark forward is done.

    int bc_init;
        //if this is a bc msg or s forward msg, used for MPI_Testall(num_isends, ...)
        //By default it's set to 0, and set to 1 for a new bc msg.
};

typedef struct isend_state isend_state;
typedef struct isend_state{
    MPI_Request req;
    MPI_Status stat;
    isend_state* prev;
    isend_state* next;
}isend_state;

typedef struct bcomm_IAR_state bcomm_IAR_state_t;
struct bcomm_IAR_state_t {
    proposal_state prop_state;
    //enum State_IAR iar_state;
    bcomm_IAR_state_t *next, *prev;
};

typedef int (*bcomm_IAR_judgement_cb_func_t)(const void *msg_buf, void *app_ctx);
typedef int (*bcomm_IAR_approval_cb_func_t)(const void *msg_buf, void *app_ctx);

typedef struct bcomm_progress_engine {
    bcomm *my_bcomm;
    //generic queues for bc
    queue queue_recv;
    queue queue_wait;
    queue queue_pickup;
    queue queue_wait_and_pickup;

    //queue queue_proposal_wait; //locally approved, wait for forwarding to be done.
    queue queue_votes_recv;//voting in process, collect votes here

    //queue queue_proposal_decided;// locally decided, or received decision, wait for cleanup.
    bcomm_GEN_msg_t
        *rcv_q_head,
        *rcv_q_tail; // Used by BC, proposal, vote, decision

    unsigned int bc_incomplete;
    unsigned int recved_bcast_cnt;
    unsigned int sent_bcast_cnt;

    //=================   IAR queues   =================
    bcomm_GEN_msg_t
        *iar_prop_rcv_q_head,
        *Iar_prop_rcv_q_tail;
    bcomm_IAR_state_t
        *prop_state_q_head,
        *prop_state_q_tail;

    bcomm_GEN_msg_t // dedicated buff for receiving votes by mpi_tag.
        *iar_vote_recv_q_head,
        *iar_vote_recv_q_tail;
    bcomm_GEN_msg_t
        *iar_decision_recv_q_head,
        *iar_decision_recv_q_tail;
    isend_state
        *iar_send_stats_head,
        *iar_send_stats_tail;
    unsigned int iar_incomplete;
    Vote vote_my_proposal_no_use;          /* Used only by an proposal-active rank. 1 for agree, 0 for decline. Accumulate votes for a proposal that I just submitted. */
    proposal_state my_own_proposal;      /* Set only when I'm a IAR starter, maintain status for my own proposal */
    proposal_state proposal_state_pool[PROPOSAL_POOL_SIZE];        /* To support multiple proposals, use a vote pool for each proposal. Use linked list if concurrent proposal number is large. */
    char* my_proposal;

    bcomm_IAR_judgement_cb_func_t judge_cb_func; //application-provided callback function for judging two proposals
    void *judge_app_ctx; //callback context
    bcomm_IAR_approval_cb_func_t approve_cb_func;
    void *approve_app_ctx;

    //debug variables
    int fwd_queued;
}bcomm_engine_t;


bcomm_GEN_msg_t* msg_new_generic(bcomm_engine_t* eng);
bcomm_GEN_msg_t* msg_new_bc(bcomm_engine_t* eng, void* buf_in, int send_size);
int msg_test_isends(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in);
int msg_wait(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in);
int msg_free(bcomm_GEN_msg_t* msg_in);
int _test_completed(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf);

// Entry point: turing gears for progress engine.
int make_progress_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t** msg_out);
int bcast_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in, enum COM_TAGS tag);
int engine_cleanup(bcomm_engine_t* eng);

int _gen_bc_msg_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);

//Generic function, to (re)post a irecv. Used by BC, IAR and all other places that need a buff to recv.
int _post_irecv_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf, enum COM_TAGS rcv_tag);


int _wait_and_pickup_queue_process(bcomm_engine_t* en, bcomm_GEN_msg_t* msg);
int _wait_only_queue_cleanup(bcomm_engine_t* eng);
int _pickup_only_queue_cleanup(bcomm_engine_t* eng);


int user_pickup_next(bcomm_engine_t* eng, bcomm_GEN_msg_t** msg_out);

// actions for proposals, votes and decisions. Called in make_progress_gen() loop.
int _iar_proposal_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);
int _iar_vote_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);
int _iar_decision_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);


int _vote_back(bcomm_engine_t* eng, proposal_state* ps, Vote vote);
int _iar_decision_bcast(bcomm_engine_t* eng, ID my_proposal_id, Vote decision);

// Submit a proposal, add it to waiting list, then return.
int _iar_submit_proposal(bcomm_engine_t* eng, char* proposal, unsigned long prop_size, ID my_proposal_id);


//Outdated
int _IAllReduce_process(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out){return 0;}


//Type for user callback functions
typedef struct proposals_ctx{

}proposals_ctx;

//Toy callback functions for proposal judging and approval.
int _proposal_judge_toy(const void *prop_1, const void *prop_2, void *app_ctx);
int _proposal_approve_toy(const void *prop_1, const void *prop_2, void *app_ctx);// compare with local proposal

//old forward function, used after IAR processing
int _forward(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out);

//New version, to replace _forward().
int _bc_forward(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg);//new version

//For irecv and other generic use
bcomm_GEN_msg_t* msg_new_generic(bcomm_engine_t* eng) {
    bcomm_GEN_msg_t* new_msg = calloc(1, sizeof(bcomm_GEN_msg_t));
    new_msg->data_buf = new_msg->buf + sizeof(int);
    new_msg->bc_isend_reqs = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Request));
    new_msg->bc_isend_stats = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Status));
    new_msg->pickup_done = 0;
    new_msg->bc_init = 0;//by default 0, when created to be a recv buf.
    new_msg->prev = NULL;
    new_msg->next = NULL;
    new_msg->send_cnt = 0;
    return new_msg;
}

bcomm_GEN_msg_t* msg_new_bc(bcomm_engine_t* eng, void* buf_in, int send_size) {
    bcomm_GEN_msg_t* new_msg = msg_new_generic(eng);
    // Set msg origin
    memcpy(new_msg->buf, &(eng->my_bcomm->my_rank), sizeof(int));

    memcpy(new_msg->data_buf, buf_in, send_size);
    new_msg->bc_init = 1;//by default. set to 0 when created to be a recv buf.
    return new_msg;
}

int msg_test_isends(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
    int completed = 0;
    //printf("%s:%u - rank = %03d, num_sends = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->send_cnt);
    MPI_Testall(msg_in->send_cnt, msg_in->bc_isend_reqs, &completed, msg_in->bc_isend_stats);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    return completed;
}

int msg_wait(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
    return MPI_Waitall(eng->my_bcomm->send_list_len, msg_in->bc_isend_reqs, msg_in->bc_isend_stats);
}

int msg_free(bcomm_GEN_msg_t* msg_in) {
    free(msg_in->bc_isend_reqs);
    free(msg_in->bc_isend_stats);
    free(msg_in);
    return 0;
}
int _queue_debug_print(queue* q){

    return q->msg_cnt;
}
int queue_append(queue* q, bcomm_GEN_msg_t* msg){
    assert(q);
    assert(msg);
    int my_rank = -1;

    if(q->head == q->tail){
        if(!q->head){ // add as the 1st msg
            msg->prev = NULL;
            msg->next = NULL;
            q->head = msg;
            q->tail = msg;
        } else {//add as 2nd msg
            msg->prev = q->tail;
            msg->next = NULL;
            q->tail->next = msg;
            q->tail = msg;
        }
    } else {
        msg->prev = q->tail;
        msg->next = NULL;
        q->tail->next = msg;
        q->tail = msg;
    }
    q->msg_cnt++;
    return 0;
}

//assume msg must be in the queue.
int queue_remove(queue* q, bcomm_GEN_msg_t* msg){
    //printf("%s:%u \n", __func__, __LINE__);
    assert(q);
    assert(msg);
    int ret = -1;
    if(q->head == q->tail){//0 or 1 msg
        if(!q->head){
            return -1;
        } else {//the only msg in the queue
            q->head = NULL;
            q->tail = NULL;
            ret = 1;
        }
    } else {//more than 1 nodes in queue
        if(msg == q->head){//remove head msg
            q->head = q->head->next;
            q->head->prev = NULL;
        } else if (msg == q->tail){// non-head msg
            q->tail = q->tail->prev;
            q->tail->next = NULL;
        } else{ // in the middle of queue
            msg->prev->next = msg->next;
            msg->next->prev = msg->prev;
        }
        ret = 1;
    }

    msg->prev = NULL;
    msg->next = NULL;


    q->msg_cnt--;
    return ret;
}
// Start progress engine, post irecv for all recv queues.
bcomm_engine_t* progress_engine_new(bcomm* my_bcomm){

    bcomm_engine_t* eng = calloc(1, sizeof(bcomm_engine_t));

    eng->my_bcomm = my_bcomm;

    eng->queue_recv.head = NULL;
    eng->queue_recv.tail = NULL;
    eng->queue_recv.msg_cnt = 0;

    eng->queue_wait.head = NULL;
    eng->queue_wait.tail = NULL;
    eng->queue_wait.msg_cnt = 0;

    eng->queue_pickup.head = NULL;
    eng->queue_pickup.tail = NULL;
    eng->queue_pickup.msg_cnt = 0;

    eng->queue_wait_and_pickup.head = NULL;
    eng->queue_wait_and_pickup.tail = NULL;
    eng->queue_wait_and_pickup.msg_cnt = 0;

    eng->recved_bcast_cnt = 0;
    eng->bc_incomplete = 0;
    eng->sent_bcast_cnt = 0;

    proposal_state new_prop_state;
    new_prop_state.pid = -1;
    new_prop_state.proposal_msg = NULL;
    new_prop_state.recv_proposal_from = -1;
    new_prop_state.vote = 0;
    new_prop_state.votes_needed = -1;
    new_prop_state.votes_recved = -1;

    eng->my_own_proposal = new_prop_state;

    bcomm_GEN_msg_t* msg_irecv_init = msg_new_generic(eng);
    eng->rcv_q_head = msg_irecv_init;
    eng->rcv_q_tail = msg_irecv_init;

    eng->fwd_queued = 0;
    //printf("%s:%u - rank = %03d, msg_new_recv = %p,  data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_irecv_init, msg_irecv_init->data_buf);
    _post_irecv_gen(eng, msg_irecv_init, ANY_TAG);
    queue_append(&(eng->queue_recv), msg_irecv_init);
    return eng;
}

//Turn the gear. Output a handle(recv_msgs_out ) to the received msg, for sampling purpose only. User should use pickup_next() to get msg.
int make_progress_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t** recv_msg_out) {
    int ret = -1;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    //========================== Bcast msg handling ==========================
    bcomm_GEN_msg_t* cur_bc_rcv_buf = eng->queue_recv.head;//eng->rcv_q_head;
    while(cur_bc_rcv_buf) {//receive and repost with tag = BCAST
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        bcomm_GEN_msg_t* msg_t = cur_bc_rcv_buf->next;
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        if(_test_completed(eng, cur_bc_rcv_buf)){//irecv complete.
            //printf("%s:%u - rank = %03d, _test_completed tests TRUE, cur_bc_rcv_buf = %p, src_rank = %d, rcv_tag = %d, error = %d\n", __func__, __LINE__,
            //        eng->my_bcomm->my_rank, cur_bc_rcv_buf, cur_bc_rcv_buf->irecv_stat.MPI_SOURCE, cur_bc_rcv_buf->irecv_stat.MPI_TAG, cur_bc_rcv_buf->irecv_stat.MPI_ERROR);
            printf("%s:%u - rank = %03d, msg received, data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank,  cur_bc_rcv_buf->data_buf);

            int recv_tag = cur_bc_rcv_buf->irecv_stat.MPI_TAG;
            queue_remove(&(eng->queue_recv), cur_bc_rcv_buf);
            bcomm_GEN_msg_t* msg_new_recv = msg_new_generic(eng);

            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            _post_irecv_gen(eng, msg_new_recv, ANY_TAG);//cur_bc_rcv_buf->irecv_stat.MPI_TAG
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

            queue_append(&(eng->queue_recv), msg_new_recv);
            //printf("%s:%u - rank = %03d, cur_bc_rcv_buf = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, cur_bc_rcv_buf);

            //printf("%s:%u - rank = %03d, tag = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, cur_bc_rcv_buf->irecv_stat.MPI_TAG);

            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            switch(recv_tag){
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                case BCAST: {
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    eng->recved_bcast_cnt++;
                    //printf("%s:%u my rank = %03d, msg received, data = [%d], enter _bc_forward...\n", __func__, __LINE__, eng->my_bcomm->my_rank,  *(int*)(cur_bc_rcv_buf->data_buf));
                    _bc_forward(eng, cur_bc_rcv_buf);
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    *recv_msg_out = cur_bc_rcv_buf;
                    break;
                }

                case IAR_PROPOSAL: {
                    //processed by a callback function, not visible to the users
                    //do not increase eng->recved_bcast_cnt
                    _iar_proposal_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                case IAR_DECISION: {
                    eng->recved_bcast_cnt++;
                    _bc_forward(eng, cur_bc_rcv_buf);//queue ops happen here
                    *recv_msg_out = cur_bc_rcv_buf;
                    _iar_decision_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                default: {
                    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    break;
                }
            }
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        }
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        cur_bc_rcv_buf = msg_t;//move cursor
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    }//loop through bc recv queue

    //============================ IAR Vote queue processing ======================
//    bcomm_GEN_msg_t* cur_vote = eng->queue_votes_recv.head;
//    while(cur_vote){
//        printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
//        bcomm_GEN_msg_t* msg_t = cur_vote->next;
//        if(_test_completed(eng, cur_vote)){
//            queue_remove(&(eng->queue_votes_recv), &cur_vote);
//            bcomm_GEN_msg_t* vote_new_recv = msg_new_generic(eng);
//            _post_irecv_gen(eng, vote_new_recv, cur_bc_rcv_buf->irecv_stat.MPI_TAG);
//            queue_append(&(eng->queue_votes_recv), &vote_new_recv);
//
//            _iar_vote_handler(eng, cur_vote);
//        }
//        cur_vote = msg_t;
//    }

    //============================ BC Wait queue processing =======================
    bcomm_GEN_msg_t* cur_wait_pickup_msg = eng->queue_wait_and_pickup.head;
    while(cur_wait_pickup_msg){
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        bcomm_GEN_msg_t* msg_t = cur_wait_pickup_msg->next;
        _wait_and_pickup_queue_process(eng, cur_wait_pickup_msg);
        cur_wait_pickup_msg = msg_t;
    }
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    //================= Cleanup wait_only and pickup_only queues ==================
    //clean up all done msgs in the wait_only queue.
    _wait_only_queue_cleanup(eng);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    //clean up all done msgs in the pickup_only queue.
    //_pickup_only_queue_cleanup(eng);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    return -1;
}

int _test_completed(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf) {
    if(!msg_buf)
        return 0;
    int completed = 0;
    MPI_Test(&(msg_buf->irecv_req), &completed, &(msg_buf->irecv_stat));

    return completed;
}

int _post_irecv_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf_in_out, enum COM_TAGS rcv_tag) {
    if(rcv_tag == ANY_TAG)
        rcv_tag = MPI_ANY_TAG;
    msg_buf_in_out->post_irecv_type =  rcv_tag;
    int ret = MPI_Irecv(msg_buf_in_out->buf, eng->my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, MPI_ANY_SOURCE, rcv_tag, eng->my_bcomm->my_comm, &(msg_buf_in_out->irecv_req));
    //msg_buf_in_out->id_debug = 1;
//    printf("%s:%u - rank = %03d, irecv posted on msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf_in_out);
    return ret;
}

int _proposal_pickup_next(){
    return -1;
}

int _iar_proposal_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in) {
    if (!eng || !recv_msg_buf_in)
        return -1;

    PBuf* pbuf = malloc(sizeof(PBuf));
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    pbuf_deserialize(recv_msg_buf_in->data_buf, pbuf);

    //add a state to waiting_votes queue.
    proposal_state* new_prop_state = malloc(sizeof(proposal_state));
    //need to read pid from msg and fill in state.
    proposalPool_init(new_prop_state, recv_msg_buf_in);
    new_prop_state->pid = pbuf->pid;
    new_prop_state->recv_proposal_from = recv_msg_buf_in->irecv_stat.MPI_SOURCE;
    new_prop_state->votes_needed = eng->my_bcomm->send_list_len;
    new_prop_state->votes_recved = 0;
    int compatible = 0;
    int i_approved = 0;
    if (eng->my_own_proposal.pid >= 0) {    // I have a active proposal waiting for processing.
        if (compatible) {    //both win
            proposalPool_proposal_add(eng->proposal_state_pool, new_prop_state);
            if (eng->my_bcomm->my_level > 0) {    //leaf rank, no forward, but need to vote right away
                _bc_forward(eng, recv_msg_buf_in);
            } else {
                _vote_back(eng, new_prop_state, 1);
            }

            i_approved = 1;
        } else { //compete
            int mine_win = 0;

            if (mine_win) {
                i_approved = 0;
                _vote_back(eng, new_prop_state, 0);
            } else { //others' win, bcast to "CANCEL" my proposal
                proposalPool_proposal_add(eng->proposal_state_pool, new_prop_state);
                if (eng->my_bcomm->my_level > 0) { //leaf rank, no forward, but need to vote right away
                    _bc_forward(eng, recv_msg_buf_in);
                } else {
                    _vote_back(eng, new_prop_state, 1);
                }
                _iar_decision_bcast(eng, eng->my_own_proposal.pid, 0);
            }
        }
    } else { //Local: do I approve the proposal
        if (i_approved) {
            //if yes, update state, move to fwd queue,
            proposalPool_proposal_add(eng->proposal_state_pool, new_prop_state);
            if (eng->my_bcomm->my_level > 0) { //leaf rank, no forward, but need to vote right away
                _bc_forward(eng, recv_msg_buf_in);
            } else {
                _vote_back(eng, new_prop_state, 1);
            }
        } else { //otherwise vote back no, discard proposal and free the msg.
            _vote_back(eng, new_prop_state, 0);
        }
    }
    free(pbuf);

    //      - wait for votes from bcast dst ranks.

    return 0;
}

int _vote_back(bcomm_engine_t* eng, proposal_state* ps, Vote vote){
    unsigned int send_len = 0;
    pbuf_serialize(ps->pid, vote, 0, NULL, eng->my_bcomm->sys_send_buf, &send_len);
    MPI_Send(eng->my_bcomm->sys_send_buf, send_len, MPI_CHAR, ps->recv_proposal_from, IAR_VOTE, eng->my_bcomm->my_comm);
    return send_len;
}

int _iar_vote_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf) {
    if (!eng || !msg_buf)
        return -1;

    //update proposal_state_queue
    //decide if all necessary votes are received, then vote back
    PBuf* vote_buf = malloc(sizeof(PBuf));
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    pbuf_deserialize(msg_buf->buf, vote_buf);        //vote: no need of + sizeof(int).

    if (vote_buf->pid == eng->my_own_proposal.pid) { //votes for my proposal

        printf("%s:%u - rank = %03d, received a vote from rank %03d for my proposal, vote = %d.\n", __func__, __LINE__,
                eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE, vote_buf->vote);
        eng->my_own_proposal.votes_recved++;
        eng->my_own_proposal.vote &= vote_buf->vote; //*(Vote*)(vote_buf->data);

        if (vote_buf->vote == 0) { //find rejection! bcast "cancel" decision right now

            _iar_decision_bcast(eng, eng->my_own_proposal.pid, 0);

        } else { //local agreed
            if (eng->my_bcomm->my_own_proposal.votes_recved == eng->my_own_proposal.votes_needed) { //all done, bcast decision.
                _iar_decision_bcast(eng, eng->my_own_proposal.pid, 1);
                pbuf_free(vote_buf);
                return 5;
            } else {                    // need more votes for my decision, continue to irecv.
                pbuf_free(vote_buf);
                return 0;
            }
        }
    } else { //Votes for proposals in the state queue
        int p_index = -1;
        int ret = proposalPool_vote_merge(eng->proposal_state_pool, vote_buf->pid, vote_buf->vote, &p_index);
        proposal_state* ps = &(eng->my_bcomm->proposal_state_pool[p_index]);
        if (ret < 0) {
            printf("Function %s:%u - rank %03d: can't merge vote, proposal not exists, pid = %d \n", __func__, __LINE__,
                    eng->my_bcomm->my_rank, vote_buf->pid);
            pbuf_free(vote_buf);
            return -1;
        } else { // Find proposal, merge completed.
            if (ret == 1) { //done collecting votes, vote back

                _vote_back(eng, ps, ps->vote);
                pbuf_free(vote_buf);
            }
            //continue to irecv votes
        }
    }

    return 0;
}


int _iar_decision_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf_in) {
    if(!eng || !msg_buf_in)
        return -1;

    //update proposal_state_queue
    PBuf* decision_buf = malloc(sizeof(PBuf));
    pbuf_deserialize(msg_buf_in->buf, decision_buf);
    int index = proposalPool_get_index(eng->proposal_state_pool, decision_buf->pid);

    if(decision_buf->vote == 0){//proposal canceled
        //some cleanup
        free(eng->proposal_state_pool[index].proposal_msg);
    } else {
        //execute proposal: a callback function
    }

    proposalPool_rm(eng->proposal_state_pool, decision_buf->pid);
    return 0;
}

int _iar_submit_proposal(bcomm_engine_t* eng, char* proposal, unsigned long prop_size, ID my_proposal_id){

    eng->my_own_proposal.pid = my_proposal_id;
    eng->my_own_proposal.proposal_msg = NULL;
    eng->my_own_proposal.vote = 1;
    eng->my_own_proposal.votes_needed = eng->my_bcomm->send_list_len;
    eng->my_own_proposal.votes_recved = 0;

    char proposal_send_buf[MSG_SIZE_MAX] = "";
    unsigned buf_len;
    if(0 != pbuf_serialize(my_proposal_id, 1, prop_size, proposal, eng->my_bcomm->user_send_buf, &buf_len)) {
        printf("pbuf_serialize failed.\n");
        return -1;
    }
    bcomm_GEN_msg_t* proposal_msg = msg_new_bc(eng, proposal_send_buf, buf_len);
    bcast_gen(eng, proposal_msg, IAR_PROPOSAL);

    return -1;
}

int _cleanup_sys_buf(bcomm_engine_t* eng){
    memset(eng->my_bcomm->sys_send_buf, 0, eng->my_bcomm->msg_size_max);
    return 0;
}

int _iar_decision_bcast(bcomm_engine_t* eng, ID my_proposal_id, Vote decision){
    unsigned send_len = 0;
    char decision_send_buf[64] = "";
    pbuf_serialize(my_proposal_id, decision, 0, NULL, decision_send_buf, &send_len);
    bcomm_GEN_msg_t* decision_msg = msg_new_bc(eng, decision_send_buf, 64);
    bcast_gen(eng, decision_msg, IAR_DECISION);
    return -1;
}

// Not used any more.
// recv_msg_buf_in_out is the iterator for the queue
int _gen_bc_msg_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in) {
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    if(!eng || !recv_msg_buf_in)
        return -1;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    if(_test_completed(eng, recv_msg_buf_in)) {
        printf("%s:%u - rank = %03d, received buf = [%s](string), origin = [%d]\n", __func__, __LINE__, eng->my_bcomm->my_rank,recv_msg_buf_in->data_buf, get_origin(recv_msg_buf_in->buf));

        eng->recved_bcast_cnt++;

        queue_remove(&(eng->queue_recv), recv_msg_buf_in);

        bcomm_GEN_msg_t* next_msg_recv_new = msg_new_generic(eng);
        _post_irecv_gen(eng, next_msg_recv_new, BCAST);
        queue_append(&(eng->queue_recv), next_msg_recv_new);

        if(recv_msg_buf_in->irecv_stat.MPI_TAG == BCAST ||
                recv_msg_buf_in->irecv_stat.MPI_TAG == IAR_DECISION){
            // Append to queue_wait_and_pickup/queue_pickup/wait inside of _bc_forward()
            _bc_forward(eng, recv_msg_buf_in);

        }else{//received a wrong type, it may be posted wrong previously
            return -1;
        }
        return 1;//received something, needs pickup
    }
    return -1;
}


// Called by the application/user, pickup a msg from the head of the queue.
// Assuming the msg will be copied and stay safe, and will be unlinked from pickup_queue.
// The user should free msg_out when it's done.
// NOTE: if this function is called in a thread different from the progress_engine thread, there will be a thread safe issue.
int user_pickup_next(bcomm_engine_t* eng, bcomm_GEN_msg_t** msg_out) {
    assert(eng);
    bcomm_GEN_msg_t* msg = eng->queue_wait_and_pickup.head;
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    if (msg) {        //wait_and_pickup empty
        printf("%s:%u - rank = %03d, wait_and_pickup msg_cnt = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_wait_and_pickup.msg_cnt);
        while (msg) {
            bcomm_GEN_msg_t* msg_t = msg->next;
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            if (!msg->pickup_done) { //find a unread msg, mark read, move to wait_fwd queue
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                queue_remove(&(eng->queue_wait_and_pickup), msg);
                // mark pickup_done in user_msg_done()
                queue_append(&(eng->queue_wait), msg);
                *msg_out = msg;
                printf("%s:%u - rank = %03d, msg = %p, *msg_out = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg, *msg_out);
                return 1;
            }
            msg = msg_t;
        }
        //no msg match in this queue. go with next queue.
    }

    msg = eng->queue_pickup.head;

    if (!(eng->queue_pickup.head)) {
        return 0;
    } else { //not empty, find the first available in queue_pickup
        printf("%s:%u - rank = %03d, pickup queue msg_cnt = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_pickup.msg_cnt);

        while (msg) {
            bcomm_GEN_msg_t* msg_t = msg->next;
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            if (!msg->pickup_done) { //return a msg
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                queue_remove(&(eng->queue_pickup), msg);
                // mark pickup_done in user_msg_done()
                *msg_out = msg;
                //printf("%s:%u - rank = %03d, msg = %p, *msg_out = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg, *msg_out);
                return 1;
            }
            msg = msg_t; // next
        }
    }
    return 0;
}

int user_msg_done(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in){
    msg_in->pickup_done = 1;
    if(msg_in->fwd_done){
        //printf("%s:%u - rank = %03d, about to free: %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in);
        free(msg_in->bc_isend_reqs);
        free(msg_in->bc_isend_stats);
        free(msg_in);
        return 1;
    }
    //still in wait queue
    return 0;
}

// Loop through all msgs in the queue, test if all isends are done.
int _wait_and_pickup_queue_process(bcomm_engine_t* eng, bcomm_GEN_msg_t* wait_and_pickup_msg){
    int ret = -1;
    //printf("%s:%u - rank = %03d, msg = %p, msg->next = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, wait_and_pickup_msg, wait_and_pickup_msg->next);
    if(msg_test_isends(eng, wait_and_pickup_msg)){//test if all isends are done
        //Remove from wait_queue, but not to free
        wait_and_pickup_msg->fwd_done = 1;
        queue_remove(&(eng->queue_wait_and_pickup), wait_and_pickup_msg);
        if(wait_and_pickup_msg->pickup_done != 1){//not been picked up yet
            queue_append(&(eng->queue_pickup), wait_and_pickup_msg);
        }
        ret = 1;
    } else {//still forwarding
        if(wait_and_pickup_msg->pickup_done){// move to wait_only queue
            queue_remove(&(eng->queue_wait_and_pickup), wait_and_pickup_msg);
            queue_append(&(eng->queue_wait), wait_and_pickup_msg);
        }
        //else: neither done forwarding, nor picked up, stay in the same queue
    }
    return 0;
}

int _wait_only_queue_cleanup(bcomm_engine_t* eng){
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    int ret = -1;
    bcomm_GEN_msg_t* cur_wait_only_msg = eng->queue_wait.head;
    while(cur_wait_only_msg){
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        bcomm_GEN_msg_t* msg_t = cur_wait_only_msg->next;
        if(msg_test_isends(eng, cur_wait_only_msg)){
            printf("%s:%u - rank = %03d, fwd is done, removing... msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, cur_wait_only_msg->data_buf);
            cur_wait_only_msg->fwd_done = 1;
            queue_remove(&(eng->queue_wait), cur_wait_only_msg);
            free(cur_wait_only_msg);
            ret = 1;
        }
        cur_wait_only_msg = msg_t;
    }
    return ret;
}

//Pickup_only queue
//int _pickup_only_queue_cleanup(bcomm_engine_t* eng){
//    int ret = -1;
//    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
//    bcomm_GEN_msg_t* msg_cur = eng->queue_pickup.head;
//    //printf("%s:%u - rank = %03d, msg_cur = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_cur);
//    while(msg_cur){
//        bcomm_GEN_msg_t* msg_t = msg_cur->next;
//        if(msg_cur->pickup_done){
//            ret =  queue_remove(&(eng->queue_pickup), &msg_cur);
//        }
//        msg_cur = msg_t;
//    }
//    //printf("%s:%u - rank = %03d, msg_cur = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_cur);
//    return ret;
//}

//msg is a recv_buf in bc_recv_buf_q, and already received data
int _bc_forward(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
    //printf("%s:%u - rank = %03d msg = %p \n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in);
    printf("%s:%u - rank = %03d, data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank,  msg_in->data_buf);
    //printf("%s:%u - rank = %03d, msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
    assert(msg_in);
    void *recv_buf;
    MPI_Status status = msg_in->irecv_stat;
    /* Increment # of messages received */
    eng->my_bcomm->bcast_recv_cnt++;

    /* Set buffer that message was received in */
    recv_buf = msg_in->buf;
    msg_in->send_cnt = 0;
    /* Check for a rank that can forward messages */
    if (eng->my_bcomm->my_level > 0) {
        int origin;
        int send_cnt;

        /* Retrieve message's origin rank */
        origin = get_origin(recv_buf);

        /* Determine which ranks to send to */
        send_cnt = 0;
        if (status.MPI_SOURCE > eng->my_bcomm->last_wall) {
            /* Send messages, to further ranks first */
            for (int j = eng->my_bcomm->send_channel_cnt; j >= 0; j--) {
                MPI_Isend(msg_in->buf, eng->my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, eng->my_bcomm->send_list[j],
                        status.MPI_TAG, eng->my_bcomm->my_comm,
                        &(msg_in->bc_isend_reqs[j]));
                send_cnt++;
                msg_in->send_cnt++;
                //printf("%s:%u my rank = %03d, forward to rank %d, data = [%d]\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list[j], *(int*)(msg_in->data_buf));
            }
            printf("%s:%u my rank = %03d, append to queue_wait_and_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
            queue_append(&(eng->queue_wait_and_pickup), msg_in);
            eng->fwd_queued++;

        } /* end if */
        else {
            int upper_bound;
            upper_bound = eng->my_bcomm->send_channel_cnt - 1; // not send to same level

            /* Avoid situation where world_size - 1 rank in non-power of 2 world_size shouldn't forward */
            if (upper_bound >= 0) {
                int any_sent = 0;
                /* Send messages, to further ranks first */
                for (int j = upper_bound; j >= 0; j--) {
                    if (check_passed_origin(eng->my_bcomm, origin, eng->my_bcomm->send_list[j]) == 0) {
                        MPI_Isend(msg_in->buf, eng->my_bcomm->msg_size_max, MPI_CHAR, eng->my_bcomm->send_list[j],
                                status.MPI_TAG, eng->my_bcomm->my_comm,
                                &(msg_in->bc_isend_reqs[j]));
                        send_cnt++;
                        msg_in->send_cnt++;
                        //printf("%s:%u my rank = %03d, forward to rank %d, data = [%d]\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list[j], *(int*)(msg_in->data_buf));
                    }
                }// end for
                //printf("%s:%u - rank = %03d, msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in);
                if(msg_in->send_cnt > 0){
                    printf("%s:%u my rank = %03d, append to queue_wait_and_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
                    queue_append(&(eng->queue_wait_and_pickup), msg_in);
                    eng->fwd_queued++;
                } else {
                    printf("%s:%u my rank = %03d, append to queue_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
                    msg_in->fwd_done = 1;
                    queue_append(&(eng->queue_pickup), msg_in);
                    eng->fwd_queued++;
                }
            } /* end if */
            else {
                printf("%s:%u - rank = %03d Something is wrong... upper_bound = %d, add to pickup queue. msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, upper_bound, msg_in->data_buf);
                msg_in->fwd_done = 1;
                queue_append(&(eng->queue_pickup), msg_in);
                eng->fwd_queued++;
            }
        } /* end else */
    } /* end if -- */
    else {
        // Leaf rank, no forward, move to pickup_only.
        //printf("%s:%u my rank = %03d, no need to forward, append to pickup queue: data = [%d]\n", __func__, __LINE__, eng->my_bcomm->my_rank, *(int*)(msg_in->data_buf));
        msg_in->fwd_done = 1;
        printf("%s:%u my rank = %03d, append to queue_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
        queue_append(&(eng->queue_pickup), msg_in);
        eng->fwd_queued++;
    }
    return 0;
}

int _iar_process_infra_q_msg(bcomm_GEN_msg_t* msg) {
    return -1;
}

int _proposal_judge_toy(const void *prop_1, const void *prop_2, void *app_ctx) {
//    if((char*)prop_1[0] >= (char*)prop_2[0]) {
//        return 1;//0
//    } else
//        return 0;//1
    return 1;
}

int _proposal_approve_toy(const void *prop_1, const void *prop_2, void *app_ctx) {
    return 1;
}


int test_gen_bcast(bcomm* my_bcomm, int buf_size, int root_rank, int cnt){
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    if(buf_size > MSG_SIZE_MAX) {
        printf("Message size too big. Maximum allowed is %d\n", MSG_SIZE_MAX);
        return -1;
    }
    bcomm_engine_t* eng = progress_engine_new(my_bcomm);
    //bcomm* my_bcomm = bcomm_init(MPI_COMM_WORLD, MSG_SIZE_MAX);


    int recved_cnt = 0;
    unsigned long start = get_time_usec();
    unsigned long time_send = 0;
    unsigned long time_recv = 5;
    bcomm_GEN_msg_t* recv_msg = NULL;
    int i = 0;
    if(my_bcomm->my_rank == root_rank) {//send
        //load data for bcast
        char buf[64] = "";

        //my_bcomm->user_send_buf = buf;

        for(int i = 0; i < cnt; i++) {
            bcomm_GEN_msg_t* send_msg = msg_new_bc(eng, buf, strlen(buf));
            sprintf(buf, "msg_No.%d", i);
            memcpy(send_msg->data_buf, buf, 64);
            printf("Rank %d bcasting: msg = [%s]\n", my_bcomm->my_rank, send_msg->data_buf);
            bcast_gen(eng, send_msg, BCAST);
            make_progress_gen(eng, &recv_msg);
            recv_msg = NULL;
        }

//        while(eng->queue_wait.head){//loop until wait queue is empty
//            //sleep(1);
//            printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
//            make_progress_gen(eng, &recv_msg);
//            //recv_msg = NULL;
//        }

        //printf("%s:%u - rank = %03d: bcast completed, no outstanding isend requests.\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    } else {//recv
        //Assume eng is initialized, no need to post now

        do{
            //receive, repost irecv.
            //printf("%s:%u - rank = %03d, recv_msgs_out = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_msg);
            recv_msg = NULL;
            make_progress_gen(eng, &recv_msg);
            //printf("%s:%u - rank = %03d, recv_msgs_out = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_msg);
            bcomm_GEN_msg_t* pickup_out = NULL;
            while(user_pickup_next(eng, &pickup_out)){
                recved_cnt++;
                //printf("%s:%u - rank = %03d pickup_out = %p \n", __func__, __LINE__, eng->my_bcomm->my_rank, pickup_out);
                printf("%s:%u - rank = %03d, received bcast msg:[%s], received %d already, expecting %d in total.\n", __func__, __LINE__,
                        my_bcomm->my_rank, pickup_out->data_buf, recved_cnt, cnt);
                //printf("%s:%u - rank = %03d, pickup_out.pickup_done = %d\n",  __func__, __LINE__, my_bcomm->my_rank, pickup_out->pickup_done);
                user_msg_done(eng, pickup_out);//free it here if(fwd_done)

            }
            //sleep(1);
        } while(recved_cnt < cnt);
    }
    unsigned long end = get_time_usec();
    MPI_Barrier(my_bcomm->my_comm);
    time_send = end - start;

    printf("Rank %d: Received %d times, average costs %lu usec/run\n", my_bcomm->my_rank, recved_cnt, (end - start)/cnt);

    //MPI_Reduce(&time_send, &time_recv, 1, MPI_UNSIGNED_LONG, MPI_MAX, root_rank, my_bcomm->my_comm);//MPI_MAX

    engine_cleanup(eng);

    return 0;
}

/* ----------------- refactoring for progress engine END ----------------- */
/* ----------------------------------------------------------------------- */
/* ----------------------------------------------------------------------- */


int proposal_compete(char* p1, char* p2);

int proposalPool_init(proposal_state* pp_in_out, bcomm_GEN_msg_t* prop_msg_in) {
    if(!pp_in_out)
        return -1;

    pp_in_out->pid = -1;
//    pp_in_out->proposal_sent_cnt = -1;
    pp_in_out->recv_proposal_from = -1;
    pp_in_out->vote = 1;
    pp_in_out->votes_needed = -1;
    pp_in_out->votes_recved = -1;
    pp_in_out->proposal_msg = prop_msg_in;
    return 0;
}

//return index, or error.
int proposalPool_proposal_add(proposal_state* pools, proposal_state* pp_in) {//add new, or merge value
    if(!pools || !pp_in)
        return -1;

    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {//exists, merge
        if(pools[i].pid == pp_in->pid) {
            printf("Function %s:%u - find proposal, pid = %d , index = %d\n", __func__, __LINE__, pp_in->pid, i);
            pools[i] = *pp_in;
            printf("Function %s:%u - confirm in array, pid = %d , index = %d\n", __func__, __LINE__, pools[i].pid, i);
            return i;
        }
    }

    // id not found, add new one.
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        if(pools[i].pid == -1) {//first available
            pools[i]= *pp_in;
//            printf("Function %s:%u - pp added, confirm in array, pid = %d , index = %d\n", __func__, __LINE__, pools[i].pid, i);
            return i;
        }
    }

    if(i == PROPOSAL_POOL_SIZE - 1) //no empty pool for use.
        return -1;

    return -2;
}

//update vote_needed field, which is set by _forward(), equals # of sends done on a proposal.
int proposalPool_proposal_setNeededVoteCnt(proposal_state* pools, ID k, int cnt) {
    if(!pools)
        return -1;

    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {//must exists, this happened after a proposal is added by _IAR_process()
        if(pools[i].pid == k) {
            pools[i].votes_needed = cnt;
            return 0;
        }
    }
    return -1;
}

int proposalPool_vote_merge(proposal_state* pools, ID k, Vote v, int* p_index_out) {
    if(!pools)
        return -1;

    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {//exists, merge
        if(pools[i].pid == k) {
            pools[i].votes_recved++;
            pools[i].vote &= v;
            *p_index_out = i;
            if(pools[i].votes_recved == pools[i].votes_needed){//votes done
                return 1;
            } else
                return 0;
        }
    }
    // id not found, need to add new one, not just merge
    return -2;
}

int proposalPool_get(proposal_state* pools, ID k, proposal_state* result) {
    if(!result)//null
        return -1;
    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        if(pools[i].pid == k) {
            *result = pools[i];
            return 0;
        }
    }
    return -1;//not found
}

int proposalPool_get_index(proposal_state* pools, ID k) {
    if(!pools)//null
        return -1;
    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        if(pools[i].pid == k)
            return i;
    }
    return -1;//not found
}

int proposalPool_rm(proposal_state* pools, ID k) {
    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        if(pools[i].pid == k) {
            pools[i].pid = -1;
            pools[i].vote = 1;
            if(!(pools[i].proposal_msg)){
                pools[i].proposal_msg = NULL;
            }
            return 0;
        }
    }
    return -1;//not found
}

int proposalPools_reset(proposal_state* pools) {
    for (int i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        pools[i].pid = -1;
        pools[i].vote = 1;
        pools[i].recv_proposal_from = -1;
        pools[i].votes_needed = -1;
        pools[i].votes_recved = -1;
        pools[i].proposal_msg = NULL;
    }
    return 0;
}

int pbuf_serialize(ID pid_in, Vote vote, unsigned int data_len_in, char* data_in, char* buf_out, unsigned int* buf_len_out) {
    if(data_len_in == 0) {
        if(data_in != NULL)
            return -1;
    }

    if(!buf_out)
        return -1;

    memcpy(buf_out, &pid_in, sizeof(ID));
    memcpy(buf_out + sizeof(ID), &vote, sizeof(Vote));
    memcpy(buf_out + sizeof(ID) + sizeof(Vote), &data_len_in, sizeof(unsigned int));

    if(data_len_in != 0)
        memcpy(buf_out + sizeof(ID) + sizeof(Vote) + sizeof(unsigned int), data_in, data_len_in);
    if(buf_len_out) {
        *buf_len_out = sizeof(ID)  /* SN */
            + sizeof(Vote)          /* vote/decision */
            + sizeof(unsigned int)  /* data_len */
            + data_len_in;          /* data */
    }
    return 0;
}

void pbuf_free(PBuf* pbuf) {
    free(pbuf->data);
    free(pbuf);
}

int pbuf_deserialize(char* buf_in, PBuf* pbuf_out) {
    if(!buf_in || !pbuf_out)
        return -1;
    memcpy(&(pbuf_out->pid), buf_in, sizeof(ID));
    memcpy(&(pbuf_out->vote), buf_in + sizeof(ID), sizeof(Vote));
    memcpy(&(pbuf_out->data_len), buf_in + sizeof(ID) + sizeof(Vote), sizeof(unsigned int));

    pbuf_out->data = malloc(MSG_SIZE_MAX - sizeof(ID));

    memcpy(pbuf_out->data, buf_in + sizeof(ID) + sizeof(Vote) + sizeof(unsigned int), pbuf_out->data_len);
    return 0;
}



typedef struct Vote_buf{
    ID pid;
    Vote vote;
}VBuf;//votes don't need bcast, only 1-to-1 send.

ID make_pid(bcomm* my_bcomm) {
    return (ID) my_bcomm->my_rank;
}

int proposal_agree(char* p1, char* p2);


int is_powerof2(int n) {
    while (n != 1 && n % 2 == 0) {
        n >>= 1;
    }
    if (n == 1) {
        return 1;
    } else {
        return 0;
    }
}

int get_level(int world_size, int rank) {
    int l;

    if (rank == 0) {
        if (is_powerof2(world_size))
            return log2(world_size) - 1;
        else
            return log2(world_size);
    }

    l = 0;
    while (rank != 0 && (rank & 0x1) == 0) {
        rank >>= 1;
        l++;
    }
    return l;
}

//This returns the closest rank that has higher level than rank 
int last_wall(int rank) {
    unsigned last_wall = rank;

    for(unsigned u = 1; u < (1024 * 1024 * 1024); u <<= 1)
        if(u & last_wall)
            return last_wall ^ u;

     return 0;//not found
}

bcomm *bcomm_init(MPI_Comm comm, size_t msg_size_max) {
    bcomm* my_bcomm;

    /* Allocate struct */
    my_bcomm = malloc(sizeof(bcomm));

    /* Copy communicator and gather stats about it */
    MPI_Comm_dup(comm, &my_bcomm->my_comm);
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    if (my_bcomm->world_size < 2) {
        printf("Too few ranks, program ended. world_size = %d\n", my_bcomm->world_size);
        return NULL;
    }
    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);

    /* Set operation counters */
    my_bcomm->my_bcast_cnt = 0;
    my_bcomm->bcast_recv_cnt = 0;

    /* Message fields */
    my_bcomm->msg_size_max = MSG_SIZE_MAX;

    /* Skip ring fields */
    my_bcomm->my_level = get_level(my_bcomm->world_size, my_bcomm->my_rank);
    if(my_bcomm->my_rank == 0)
        my_bcomm->last_wall = pow(2, my_bcomm->my_level);
    else
        my_bcomm->last_wall = last_wall(my_bcomm->my_rank);
    my_bcomm->world_is_power_of_2 = is_powerof2(my_bcomm->world_size);

    /* Set up send fields */
    my_bcomm->send_channel_cnt = my_bcomm->my_level;
    my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;
    my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));
    if (my_bcomm->world_is_power_of_2) {
        for (int i = 0; i < my_bcomm->send_list_len; i++)
            my_bcomm->send_list[i] = (int) (my_bcomm->my_rank + pow(2, i)) % my_bcomm->world_size;
    } 
    else { // non 2^n world size
        for (int i = 0; i < my_bcomm->send_list_len; i++) {
            int send_dest = (int) (my_bcomm->my_rank + pow(2, i));

            /* Check for sending to ranks beyond the end of the world size */
            if (send_dest >= my_bcomm->world_size) {
                if (my_bcomm->my_rank == (my_bcomm->world_size - 1)) {
                    my_bcomm->send_channel_cnt = 0;
                    my_bcomm->send_list[0] = 0;
                } /* end if */
                else {
                    my_bcomm->send_channel_cnt = i;
                    my_bcomm->send_list[i] = 0;
                } /* end else */

                /* Reset # of valid destinations in array */
                my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;

                /* Break out of loop now, we're finished with the destinations to send to */
                break;
            } /* end if */
            else
                my_bcomm->send_list[i] = send_dest;
        }
    }
    my_bcomm->fwd_send_cnt[0] = 0;
    my_bcomm->fwd_send_cnt[1] = 0;
    //my_bcomm->fwd_isend_reqs[0] = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    //my_bcomm->fwd_isend_reqs[1] = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    //my_bcomm->fwd_isend_stats[0] = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    //my_bcomm->fwd_isend_stats[1] = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    my_bcomm->bcast_send_cnt = 0;
    //my_bcomm->bcast_isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    //my_bcomm->bcast_isend_stats = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    //my_bcomm->send_buf = malloc(sizeof(int) + msg_size_max);
    //my_bcomm->send_buf_internal = malloc(sizeof(int) + msg_size_max);
    //memcpy(my_bcomm->send_buf, &my_bcomm->my_rank, sizeof(int));
//    memcpy(my_bcomm->send_buf_internal, &my_bcomm->my_rank, sizeof(int));
    my_bcomm->user_send_buf = ((char *)my_bcomm->send_buf) + sizeof(int);
    my_bcomm->sys_send_buf = ((char *)my_bcomm->send_buf_internal) + sizeof(int);

    /* Set up receive fields */
//    my_bcomm->recv_buf[0] = (char*) malloc(sizeof(int) + msg_size_max);
//    my_bcomm->recv_buf[1] = (char*) malloc(sizeof(int) + msg_size_max);
    my_bcomm->curr_recv_buf_index = 0;

    /* Set up I_All_Reduce fields */
//    my_bcomm->IAR_recv_buf = (char*)malloc(sizeof(int) + sizeof(ID) + msg_size_max);                 /* IallReduce recv buf */

    my_bcomm->IAR_active = 0;
    proposalPools_reset(my_bcomm->proposal_state_pool);

//    my_bcomm->my_proposal = (char*)malloc(msg_size_max);                  /* This is used to compare against received proposal, shuold be updated timely */
//    my_bcomm->send_buf_my_vote = (char*)malloc(sizeof(int) + sizeof(ID) + sizeof(Vote));
    my_bcomm->recv_vote_cnt = 0;

    return my_bcomm;
}

int get_origin(void* buf_in) {
    return *((int*) buf_in);
}

// Event progress tracking
int check_passed_origin(const bcomm* my_bcomm, int origin_rank, int to_rank) {
    int my_rank = my_bcomm->my_rank;

    if (to_rank == origin_rank)
        return 1;

    if (my_rank >= origin_rank) {
        if (to_rank > my_rank)
            return 0;
        else {    //to_rank < my_rank
            if (to_rank >= 0 && to_rank < origin_rank)
                return 0;
            else
                //to_rank is in [origin_rank, my_rank)
                return 1;
        }
    } else { // 0 < my_rank < origin_rank
        if (to_rank > my_rank && to_rank < origin_rank)
            return 0;
        else
            return 1;
    }
}

// Used by all ranks
int bufer_maintain_irecv(bcomm* my_bcomm) {
    /* Re-post receive, for next message */
    my_bcomm->curr_recv_buf_index = !my_bcomm->curr_recv_buf_index;

    /* If there are outstanding messages being forwarded from this buffer, wait for them now */
    if (my_bcomm->fwd_send_cnt[my_bcomm->curr_recv_buf_index] > 0) {

        MPI_Waitall(my_bcomm->fwd_send_cnt[my_bcomm->curr_recv_buf_index],
                my_bcomm->fwd_isend_reqs[my_bcomm->curr_recv_buf_index],
                my_bcomm->fwd_isend_stats[my_bcomm->curr_recv_buf_index]);

        my_bcomm->fwd_send_cnt[my_bcomm->curr_recv_buf_index] = 0;
    } /* end if */

    MPI_Irecv(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index], my_bcomm->msg_size_max + sizeof(int), MPI_CHAR,
            MPI_ANY_SOURCE, MPI_ANY_TAG, my_bcomm->my_comm, &my_bcomm->irecv_req);
    //printf("%s:%u - rank = %03d: ----------------------------------- irecv() reposted, req = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->irecv_req);
    return -1;
}

int bcast(bcomm* my_bcomm, enum COM_TAGS tag);

int _IAllReduce_StarterVote(bcomm* my_bcomm, Vote vote_in, ID pid) {
    //PBuf* vote_buf = malloc(sizeof(PBuf));
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    int pp_index = proposalPool_get_index(my_bcomm->proposal_state_pool, pid);

    if(-1 == pp_index) {
        printf("%s:%u - rank = %03d: starter can't find pid in my proposal pools, pid = %d, index = %d\n", __func__, __LINE__, my_bcomm->my_rank,pid, pp_index);
        return -1;
    }

    printf("%s:%u - rank = %03d: starter send vote for pid = %d, vote = %d index = %d, send to rank %03d\n", __func__, __LINE__, my_bcomm->my_rank, pid, vote_in, pp_index, my_bcomm->proposal_state_pool[pp_index].recv_proposal_from);
//    vote_buf->data_len = 0;
//    vote_buf->data = NULL;//malloc(vote_buf->data_len);
//    memcpy(vote_buf->data, &vote_in, sizeof(Vote));
//    vote_buf->pid = pid;

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    unsigned int send_len;
    pbuf_serialize(pid, vote_in, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->proposal_state_pool[pp_index].recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    bufer_maintain_irecv(my_bcomm);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    //pbuf_free(vote_buf);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    return 0;
}
//Decision format: ID:Vote:Content.
//TODO: need to refactor to reuse PBuf.
//OLD.
int _IAllReduce_bacast_decision(bcomm* my_bcomm, ID pid, char* proposal, int pp_len, Vote decision) {
    //memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(unsigned int), &my_bcomm->my_own_proposal.vote, sizeof(Vote));
    if(!proposal) {
        if(decision == 1)
            return -1;
    }
    memcpy(my_bcomm->user_send_buf, &pid, sizeof(ID));
    memcpy(my_bcomm->user_send_buf + sizeof(ID), &decision, sizeof(Vote));

    if(decision == 1) {
        memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(Vote), proposal, pp_len);
    }

    return bcast(my_bcomm, IAR_DECISION);
}

//int _IAllReduce_process(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out) {
//    // All msg that without a proposal or vote tag (such as decision) will cause return 0 and captured by _forward().
////    printf("%s:%u - rank = %03d, _IAllReduce_process starts, status.MPI_TAG = %d, status.source = %d\n",
////            __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG, status.MPI_SOURCE);
//
//    if(status.MPI_TAG == IAR_VOTE) {//collect vote and up stream
//        //  - If it's a vote for me (only when its IAR_active), only merge, but no upstreaming
//        //  - else
//        //      - if non-leaf rank: merge and wait until all votes collected, then up stream
//        //      - if it is a leaf rank, vote back directly.
//
//        if(recv_buf_out)// not NULL
//            *recv_buf_out = (char *) my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];
//
//        //my_bcomm->recv_vote_cnt ++;
//
//        PBuf* vote_buf = malloc(sizeof(PBuf));
//        pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index], vote_buf);// + sizeof(int). IAR_VOTE doesn't need origin
//        printf("%s:%u - rank = %03d, tag = %d, received a vote from rank %03d, for pid = %d\n",
//                __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG,  status.MPI_SOURCE, vote_buf->pid);
//
//        if(my_bcomm->IAR_active == 1) {//If it's a vote for my proposal (only when its IAR_active), only merge, but no upstreaming
//            if(vote_buf->pid == my_bcomm->my_own_proposal.pid) {
//                printf("%s:%u - rank = %03d, received a vote from rank %03d for my proposal, vote = %d.\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, vote_buf->vote);
//                my_bcomm->my_own_proposal.votes_recved++;
//                my_bcomm->my_own_proposal.vote &= vote_buf->vote;//*(Vote*)(vote_buf->data);
//                if(my_bcomm->my_own_proposal.votes_recved == my_bcomm->my_own_proposal.votes_needed) {//all done, bcast decision.
//                    //vote 1 to
//                    bufer_maintain_irecv(my_bcomm);
//                    pbuf_free(vote_buf);
//                    return 5;
//                } else {// need more votes for my decision, continue to irecv.
//                    bufer_maintain_irecv(my_bcomm);
//                    pbuf_free(vote_buf);
//                    return 4;
//                }
//            }
//            //not for me: same as Passive ranks
//        }
//
//        //Passive ranks' response for votes: merge and report upwards
//
//        proposal_state pp;
//        //check if exist, if yes, increase it by merging. if no, it's an error, since a proposal_pool should exist before a corresponding vote arrive.
//        // receive a vote means I must received a proposal before.
//        int t;
//        if(0 != proposalPool_vote_merge(my_bcomm->proposal_state_pool, vote_buf->pid, vote_buf->vote, &t)) {
//            printf("Function %s:%u - rank %03d: can't merge vote, proposal not exists, pid = %d \n", __func__, __LINE__, my_bcomm->my_rank, vote_buf->pid);
//            bufer_maintain_irecv(my_bcomm);
//            pbuf_free(vote_buf);
//            return -1;
//        }
//        int p_index = proposalPool_get_index(my_bcomm->proposal_state_pool, vote_buf->pid);
////        printf("Function %s:%u - rank %03d: completed merging vote, pid = %d, votes_needed = %d, votes_recved = %d \n", __func__, __LINE__, my_bcomm->my_rank, vote_buf->pid, my_bcomm->my_proposal_pools[p_index].votes_needed, my_bcomm->my_proposal_pools[p_index].votes_recved);
//
//        if(-1 == p_index) {
//            printf("Function %s:%u - rank %03d: can't find proposal. \n", __func__, __LINE__, my_bcomm->my_rank);
//            bufer_maintain_irecv(my_bcomm);
//            pbuf_free(vote_buf);
//            return -1;
//        }
//
//        //printf("%s:%u - rank %03d received a vote: %d, now my vote = %d\n", __func__, __LINE__, my_bcomm->my_rank, *(Vote*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(SN)+ sizeof(unsigned int)), my_bcomm->my_vote);
//        printf("%s:%u - rank %03d: Passive. received a vote, pid = %d, already got %d votes, need %d in total\n", __func__, __LINE__, my_bcomm->my_rank, vote_buf->pid, my_bcomm->proposal_state_pool[p_index].votes_recved, my_bcomm->proposal_state_pool[p_index].votes_needed);
//        if(my_bcomm->proposal_state_pool[p_index].votes_recved == my_bcomm->proposal_state_pool[p_index].votes_needed) {//all votes are received, report to predecessor
//            printf("Function %s:%u - rank %03d: all votes (%d) are received, report to predecessor rank %03d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->proposal_state_pool[p_index].votes_needed, my_bcomm->proposal_state_pool[p_index].recv_proposal_from);
//            vote_buf->vote = my_bcomm->proposal_state_pool[p_index].vote;
//            unsigned int send_len;
//            pbuf_serialize(vote_buf->pid, vote_buf->vote, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
//            MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->proposal_state_pool[p_index].recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
//            bufer_maintain_irecv(my_bcomm);
//            pbuf_free(vote_buf);
//            return 3;//received all votes and reported to parent, continue
//        }
//
//        bufer_maintain_irecv(my_bcomm);
//        pbuf_free(vote_buf);
//        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//        return 2;//received a vote, but expecting more
//    }// end vote handling
//
//    if(status.MPI_TAG == IAR_PROPOSAL) {// new proposal, down stream
//        if(recv_buf_out)// not NULL
//            *recv_buf_out = (char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]+ sizeof(int);
//
//        PBuf* pbuf = malloc(sizeof(PBuf));
//        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//        pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);
//
//        int origin = get_origin((char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]);
//        printf("%s:%u - rank = %03d, received a proposal from rank %0d:(%d:%s), data_len = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->data, pbuf->data_len);
//        //my_bcomm->recv_proposal_from = status.MPI_SOURCE;
//        //my_bcomm->proposal_sent_cnt = 0;
//
//
//        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//        if (0 == proposal_agree(my_bcomm->my_proposal, pbuf->data)) {//local declined, up stream to the parent, no need of collecting votes.
//            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//            //set vote
//            Vote tmp_v = 0;
//            //my_bcomm->vote_my_proposal_no_use = tmp_v;
//            //proposalPool_vote_merge(my_bcomm->my_vote_pools, pbuf->pid, 0);
//
//            unsigned int send_len;
//            pbuf_serialize(pbuf->pid, tmp_v, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
//
//            MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, status.MPI_SOURCE, IAR_VOTE, my_bcomm->my_comm);// sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
//            printf("%s:%u - rank = %03d, local declined, sending no vote to rank %03d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE);
//
//            bufer_maintain_irecv(my_bcomm);
//            pbuf_free(pbuf);
//            return 1;//proposal declined locally, reported, continue
//        } else {//local approved
//
//            if(my_bcomm->IAR_active == 0) {//Not an IAR starter, downstream/forward if not a leaf rank.
//                memcpy(my_bcomm->send_buf_my_vote, my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), sizeof(ID));//copy pid
//
//                Vote tmp_v = 1;
//                //my_bcomm->vote_my_proposal_no_use = tmp_v;
//                proposal_state pp;
//                proposalPool_init(&pp, NULL);
//                pp.pid = pbuf->pid;
//                pp.recv_proposal_from = status.MPI_SOURCE;
//                pp.votes_recved = 0;
//                pp.vote = 1;
//
//                //non-leaf rank need to report after collecting all children votes, handled by IAR_VOTE tag branch.
//                //printf("%s:%u - rank = %03d: adding pp...\n", __func__, __LINE__, my_bcomm->my_rank);
//                int paindex = proposalPool_proposal_add(my_bcomm->proposal_state_pool, &pp);
//                //printf("%s:%u - rank = %03d: getting pp index...\n", __func__, __LINE__, my_bcomm->my_rank);
//                //int pindex = proposalPool_get_index(my_bcomm->my_proposal_pools,pbuf->pid);
//                //printf("%s:%u - rank = %03d, non-starter, non-leaf rank added a propolsal, pid = %d , pindex = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_proposal_pools[pindex].pid, pindex);
//
//                // //leaf rank don't report until all children's votes are received, or if it's a leaf rank, reports directly.
//                if(my_bcomm->send_channel_cnt == 0) {//leaf rank
//                    printf("%s:%u - rank = %03d: leaf rank sending vote(%d) for pid(%d) to rank %03d...\n", __func__, __LINE__, my_bcomm->my_rank, tmp_v, pp.pid, status.MPI_SOURCE);
//                    unsigned int send_len;
//                    pbuf_serialize(pp.pid, tmp_v, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
//                    MPI_Request isend_rea;
//                    int error_val = MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, status.MPI_SOURCE, IAR_VOTE, my_bcomm->my_comm);//, &isend_rea, //sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
//                    PBuf* pbuf_t = malloc(sizeof(PBuf));
//                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//                    pbuf_deserialize(my_bcomm->send_buf_my_vote, pbuf_t);
////                    printf("%s:%u - rank = %03d: verify send_buf content after sync MPI_Send: "
////                            "pid = %d, vote = %d, data_len = %d, data = %s, MPI_Send return = %d \n\n", __func__, __LINE__, my_bcomm->my_rank,
////                            pbuf_t->pid, pbuf_t->vote, pbuf_t->data_len, pbuf_t->data, error_val);
//                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//                    bufer_maintain_irecv(my_bcomm);
//                    pbuf_free(pbuf);
//                    return 4;//leaf rank agreed, no forward, continue.
//                } else {//non leaf rank, non starter, always forward proposals and decisions
//                    //bufer_maintain_irecv(my_bcomm); don't do this if returning 0, buffer will be used for later forwarding.
//
//                    //if passed origin: no forward, need to vote
//
//                    //if not passed, forward, and wait for vote (return 0 and do nothing)
//                    if(check_passed_origin(my_bcomm, origin, my_bcomm->send_list[0]) != 0) {//passed
//                        int ret = _IAllReduce_StarterVote(my_bcomm, 1, pbuf->pid);
//                        if(ret != 0)
//                            bufer_maintain_irecv(my_bcomm);
//                        pbuf_free(pbuf);
//                        return 4;//leaf rank agreed, no forward, continue.
//                    }
//
//                    pbuf_free(pbuf);
//                    printf("%s:%u - rank = %03d: non-leaf forwarding proposal\n", __func__, __LINE__, my_bcomm->my_rank);
//                    return 0;
//                }
//
//            } else { // starter, all ranks.
//                proposal_state pp;
//                proposalPool_init(&pp, NULL);
//                pp.pid = pbuf->pid;
//                pp.recv_proposal_from = status.MPI_SOURCE;
//                pp.votes_recved = 0;
//
//                int winner = proposal_compete(my_bcomm->my_proposal, pbuf->data);
//                if (winner == 0) { // I win, vote no to others.
//                    pp.vote = 0;
//                    //non-leaf rank need to report after collecting all children votes, handled by IAR_VOTE tag branch.
//                    proposalPool_proposal_add(my_bcomm->proposal_state_pool, &pp);
//                    printf("%s:%u - rank = %03d: starter: I win (%s), vote no to others(%s).\n", __func__, __LINE__,
//                            my_bcomm->my_rank, my_bcomm->my_proposal, pbuf->data);
//                    int ret = _IAllReduce_StarterVote(my_bcomm, 0, pbuf->pid);
//                    if(ret != 0)
//                        bufer_maintain_irecv(my_bcomm);
//                    pbuf_free(pbuf);
//                    return 1;
//                } else { // others' win, forward if I'm not a leaf
//
//                    //TODO: bcast no decision for my proposal, return 6, but still need to continue forwarding.
//                    my_bcomm->my_own_proposal.vote = 0;
//                    //my_bcomm->my_own_proposal.votes_recved = my_bcomm->my_own_proposal.votes_needed; //condition to end loop, too early
//                    //_IAllReduce_bacast_decision(my_bcomm, my_bcomm->my_own_proposal.pid, NULL, 0, 0);
//                    pp.vote = 1;
//                    //non-leaf rank need to report after collecting all children votes, handled by IAR_VOTE tag branch.
//                    proposalPool_proposal_add(my_bcomm->proposal_state_pool, &pp);
//
//                    // add proposal
//
//                    int pindex = proposalPool_get_index(my_bcomm->proposal_state_pool, pbuf->pid);
////                    printf("%s:%u - rank = %03d, non-starter, non-leaf rank added a propolsal, pid = %d \n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_proposal_pools[pindex].pid);
//
//                    if (my_bcomm->send_channel_cnt != 0) {                    //non leaf, forward
//
//                        if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[0]) == 0) { //no passed origin, forward and wait for children's vote
//                            printf("%s:%u - rank = %03d: starter: others' win, non-leaf forwarding...\n", __func__, __LINE__, my_bcomm->my_rank);
//                            printf("%s:%u - rank = %03d, received a proposal from rank %03d:(%d:%s), data_len = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->data, pbuf->data_len);
//                            //bufer_maintain_irecv(my_bcomm);
//                            pbuf_free(pbuf);
//                            return 0;       // others' proposal will be forwarded. return 0 will be caught by _forward();
//                        } else {                    //passed, no forward but need to vote
//
//                            if (0 != _IAllReduce_StarterVote(my_bcomm, 1, pbuf->pid)) {
//                                printf("%s:%u - rank = %03d: starter: others' win, non-leaf can't forward (passed origin), vote failed: can't find pid in pool, return 9\n", __func__, __LINE__, my_bcomm->my_rank);
//                                bufer_maintain_irecv(my_bcomm);
//                                pbuf_free(pbuf);
//                                return 9;                    //unknown error
//                            }
//                            printf("%s:%u - rank = %03d: starter: others' win, non-leaf can't forward (passed origin), return 9 and don't need to do anything\n", __func__, __LINE__, my_bcomm->my_rank);
//                            //bufer_maintain_irecv(my_bcomm);
//                            pbuf_free(pbuf);
//                            return 9;                    //unknown error
//                        }
//                    } else {                    //leaf, no forward, but need to vote
//                        printf("%s:%u - rank = %03d: starter: others' win, leaf rank, no forward. voting yes.\n",
//                                __func__, __LINE__, my_bcomm->my_rank);
//
//                        if (0 != _IAllReduce_StarterVote(my_bcomm, 1, pbuf->pid)) {
//                            printf("%s:%u - rank = %03d: starter: others' win, leaf don't need to forward , vote failed: can't find pid in pool, return 9\n", __func__, __LINE__, my_bcomm->my_rank);
//                            bufer_maintain_irecv(my_bcomm);
//                            pbuf_free(pbuf);
//                            return 9;                    //unknown error
//                        }
//                        //bufer_maintain_irecv(my_bcomm);
//                        pbuf_free(pbuf);
//                        return 4;
//                    }
//                }
//            }
//        }
//        // else case: regular forward
//    }
//
//    if(status.MPI_TAG == IAR_DECISION) {//
//
//        PBuf* pbuf = malloc(sizeof(PBuf));
//        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//        pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);
//
//        int origin = get_origin((char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]);
////        printf("%s:%u - rank = %03d, received a decision from rank %03d:(%d:%s), data_len = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->data, pbuf->data_len);
//        pbuf_free(pbuf);
//        return 0;
//    }
//    //neither proposal nor vote.
//    return 0;
//}

// status is needed due to the need of tag and source(where it's received).
int _forward(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out) {
    //printf("%s:%u - rank = %03d, tag = %d \n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG);
    void *recv_buf;

    /* Increment # of messages received */
    my_bcomm->bcast_recv_cnt++;

    /* Set buffer that message was received in */
    recv_buf = my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];
    PBuf* pbuf = malloc(sizeof(PBuf));
//    printf("%s:%u - rank = %03d: recv_buf: origin = %d, pid = %d, vote = %d, data = %s\n",
//            __func__, __LINE__, my_bcomm->my_rank,
//            *((int*) recv_buf), *((int*) (recv_buf + sizeof(int))), *((int*) (recv_buf + 2 * sizeof(int))), (char*) (recv_buf + 3 * sizeof(int)));
    pbuf_deserialize(recv_buf + sizeof(int), pbuf);//still within irecv() scope, need offset origin.
    //if(status.MPI_TAG == 2)
//    printf("%s:%u - rank = %03d: recv_buf: origin = %d, pid = %d, vote = %d, data_len = %d, data = %s\n",
//            __func__, __LINE__, my_bcomm->my_rank,
//            *((int*) recv_buf), pbuf->pid, pbuf->vote, pbuf->data_len, pbuf->data);
    /* Check for a rank that can forward messages */
    if (my_bcomm->my_level > 0) {
        int origin;
        int send_cnt;

        /* Retrieve message's origin rank */
        origin = get_origin(recv_buf);
//        printf("%s:%u - rank = %03d, "
//                "before forward from rank %03d:(pid = %d, vote = %d, data = %s),"
//                " tag = %d, origin = %d\n",
//                __func__, __LINE__, my_bcomm->my_rank,
//                status.MPI_SOURCE, pbuf->pid, pbuf->vote, pbuf->data, status.MPI_TAG, origin);
        /* Determine which ranks to send to */
        send_cnt = 0;
        if (status.MPI_SOURCE > my_bcomm->last_wall) {
            /* Send messages, to further ranks first */
            for (int j = my_bcomm->send_channel_cnt; j >= 0; j--) {
                //printf("%s:%u - rank = %03d, source > last_wall, sending to rank %03d now...\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->send_list[j]);
                MPI_Isend(recv_buf, my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, my_bcomm->send_list[j],
                        status.MPI_TAG, my_bcomm->my_comm,
                        &(my_bcomm->fwd_isend_reqs[my_bcomm->curr_recv_buf_index][send_cnt]));
                send_cnt++;
            }
        } /* end if */
        else {
            int upper_bound;

            upper_bound = my_bcomm->send_channel_cnt - 1; // not send to same level

            /* Avoid situation where world_size - 1 rank in non-power of 2 world_size shouldn't forward */
            if (upper_bound >= 0) {

                /* Send messages, to further ranks first */
                for (int j = upper_bound; j >= 0; j--) {
//                    printf("%s:%u - rank = %03d, upper_bound >= 0\n", __func__, __LINE__, my_bcomm->my_rank);
                    if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
//                        printf("%s:%u - rank = %03d, not passed, sending to rank %03d now...\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->send_list[j]);
                        MPI_Isend(recv_buf, my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, my_bcomm->send_list[j],
                                status.MPI_TAG, my_bcomm->my_comm,
                                &(my_bcomm->fwd_isend_reqs[my_bcomm->curr_recv_buf_index][send_cnt]));
                        send_cnt++;
                    }
                }
            } /* end if */
        } /* end else */

        /* Update # of outstanding messages being sent for bcomm */
        my_bcomm->fwd_send_cnt[my_bcomm->curr_recv_buf_index] = send_cnt;//???

        if(status.MPI_TAG == IAR_PROPOSAL) {
            PBuf* pbuf2 = malloc(sizeof(PBuf));
            pbuf_deserialize(recv_buf + sizeof(int), pbuf2);

            int pp_index =  proposalPool_get_index(my_bcomm->proposal_state_pool, pbuf2->pid);
            if(-1 == pp_index) {
                return -1;
            }
            my_bcomm->proposal_state_pool[pp_index].votes_needed = send_cnt;
            pbuf_free(pbuf2);
        }

//        if (my_bcomm->proposal_sent_cnt == 0)
//            my_bcomm->proposal_sent_cnt = send_cnt;



    } /* end if */

    /* Return pointer to user data in current receive buffer */
    if (recv_buf_out) // not NULL
        *recv_buf_out = ((char *) recv_buf) + sizeof(int);
    pbuf_free(pbuf);
    bufer_maintain_irecv(my_bcomm);//release recv buf.
    return 0;
}

int make_progress(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out) {
    //printf("%s:%u - rank = %03d, status.tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG);
    int ret = _IAllReduce_process(my_bcomm, status, recv_buf_out);
    if(ret != 0)
        return ret;

    //BCAST and DECISION tags are treated in the same way.
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    PBuf* pbuf = malloc(sizeof(PBuf));
    pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);
//    printf("%s:%u - rank = %03d,  right before forward from rank %03d:(%d:%s), data_len = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->data, pbuf->data_len);

    //only when ret == 0, and need to make sure don't call buffer maintain before here.
    _forward(my_bcomm, status, recv_buf_out);
    return 0;
}

int irecv_wrapper(bcomm* my_bcomm, char** recv_buf_out, MPI_Status* stat_out) {//int* recved_tag_out,
    MPI_Status stat;
    int completed = 0;
    MPI_Test(&my_bcomm->irecv_req, &completed, &stat);
    if(stat_out)
        *stat_out = stat;

    if(completed) {
//        if(recved_tag_out)
//            *recved_tag_out = stat_out->MPI_TAG;

        //printf("%s:%u - rank = %03d, irecv complete, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, stat.MPI_TAG, stat.MPI_SOURCE, stat.MPI_ERROR);
//        if(my_bcomm->my_rank == 2) {
//            printf("%s:%u ----------------------------------- rank = %03d, irecv complete, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, stat.MPI_TAG, stat.MPI_SOURCE, stat.MPI_ERROR);
//        }
        if(stat.MPI_TAG == -1) {

            //printf("%s:%u - rank = %03d, complete, my_bcomm->irecv_req = %d, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->irecv_req, stat.MPI_TAG, stat.MPI_SOURCE, stat.MPI_ERROR);

            return -1;
        }
//        printf("%s:%u - rank = %03d, ================================ MPI_Irecv completed, req = %d, stat.MPI_TAG = %d, source = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->irecv_req, stat.MPI_TAG, stat.MPI_SOURCE);
        return make_progress(my_bcomm, stat, recv_buf_out);
    }
    return -1;//not complete
}

int bcast_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in, enum COM_TAGS tag) {

    bcomm* my_bcomm = eng->my_bcomm;
    msg_in->bc_init = 1; // just to ensure.
    msg_in->pickup_done = 1; // bc msg doesn't need pickup.
//printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--) {

//printf("%s:%u - rank = %03d, i = %d\n", __func__, __LINE__, my_bcomm->my_rank, i);

        MPI_Isend(msg_in->buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm,
            &(msg_in->bc_isend_reqs[i]));
        msg_in->send_cnt++;
        //printf("%s:%u my rank = %03d, bcast to rank %d, data = [%d]\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list[i], *(int*)(msg_in->data_buf));
    }
    // bc has no need of local pickup
    queue_append(&(eng->queue_wait), msg_in);
    //_append_wait_q(eng, msg_in);
    eng->sent_bcast_cnt++;

    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    return 0;
}


// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_bcomm, enum COM_TAGS tag) {
    /* If there are outstanding messages being broadcast, wait for them now */
    if (my_bcomm->bcast_send_cnt > 0) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
        my_bcomm->bcast_send_cnt = 0;
    } /* end if */
    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--) {
        //printf("%s:%u - rank = %03d, bcast to rank %03d, tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->send_list[i], tag);
        MPI_Isend(my_bcomm->send_buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag,
                my_bcomm->my_comm, &my_bcomm->bcast_isend_reqs[i]); // my_bcomm->my_comm
    }
    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    return 0;
}

int engine_cleanup(bcomm_engine_t* eng){
//    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat_out1, stat_out2;
    int done = 0;
    bcomm* my_bcomm = eng->my_bcomm;
    bcomm_GEN_msg_t* recv_msg;

//    printf("%s:%u - rank = %03d, Before IallReduce: recved_bcast_cnt = %d, sent_bcast_cnt = %d, my_bcast_cnt = %d, total_bcast = %d\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, eng->recved_bcast_cnt, eng->sent_bcast_cnt, my_bcomm->my_bcast_cnt, total_bcast);
    MPI_Iallreduce(&(eng->sent_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, eng->my_bcomm->my_comm, &req);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    do {
        MPI_Test(&req, &done, &stat_out1);// test for MPI_Iallreduce.
        if (!done) {
            make_progress_gen(eng, &recv_msg);
        }
    } while (!done);

    // Core cleanup section
    while (eng->recved_bcast_cnt + eng->sent_bcast_cnt < total_bcast) {
        make_progress_gen(eng, &recv_msg);
    }

    printf("%s:%u - rank = %03d, recved_bcast_cnt = %d, sent_bcast_cnt = %d, total_bcast = %d\n",
            __func__, __LINE__, eng->my_bcomm->my_rank, eng->recved_bcast_cnt, eng->sent_bcast_cnt, total_bcast);

    recv_msg = NULL;
    bcomm_GEN_msg_t* pickup_out = NULL;
    while(user_pickup_next(eng, &pickup_out)){
        total_pickup++;
        printf("%s:%u - rank = %03d, pickup_out msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, pickup_out->data_buf);
    }


    printf("%s:%u - rank = %03d: FINAL RESULT:"
            " eng->recved_bcast_cnt = %d, eng->sent_bcast_cnt = %d, total_bcast = %d\n",
            __func__, __LINE__, my_bcomm->my_rank,
            eng->recved_bcast_cnt, eng->sent_bcast_cnt, total_bcast);

//    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    free(eng->my_bcomm);
    free(eng);
    return 0;
}

int bcomm_teardown(bcomm* my_bcomm, enum COM_TAGS tag) {
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat_out1, stat_out2;
    int done;
    char *recv_buf;
    int recv_cnt;
    int ret;
    /* If there are outstanding messages being broadcast, wait for them now */
    if(my_bcomm->bcast_send_cnt > 0) {
        MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
        my_bcomm->bcast_send_cnt = 0;
    } /* end if */

    /* Retrieve the # of broadcasts from all ranks */
    MPI_Iallreduce(&(my_bcomm->my_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, my_bcomm->my_comm, &req);
    printf("%s:%u - rank = %03d, bcomm_teardown start to wait for MPI_Iallreduce.\n", __func__, __LINE__, my_bcomm->my_rank);
    /* Forward messages until all ranks have participated in allreduce for total braoadcast count */
    do {
        MPI_Test(&req, &done, &stat_out1);// test for MPI_Iallreduce.
        if (!done) {
            //recv_forward(my_bcomm, &recv_buf, NULL);
            //printf("%s:%u - rank = %03d, bcomm_teardown: looping to wait for MPI_Iallreduce done.\n", __func__, __LINE__, my_bcomm->my_rank);
            ret = irecv_wrapper(my_bcomm, &recv_buf, &stat_out2);
            if(ret != -1) {
                PBuf* pbuf = malloc(sizeof(PBuf));
                pbuf_deserialize(recv_buf, pbuf);
                printf("%s:%u - rank = %03d, in MPI_Iallreduce loop, received tag(%d) from rank %03d, pid = %d, vote = %d\n", __func__, __LINE__, my_bcomm->my_rank, stat_out2.MPI_TAG, stat_out2.MPI_SOURCE, pbuf->pid, pbuf->vote);
            }
        }
    } while (!done);

    printf("%s:%u - rank = %03d, bcomm_teardown: MPI_Iallreduce done. my_bcomm->bcast_recv_cnt = %d, my_bcomm->my_bcast_cnt = %d, total_bcast = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->bcast_recv_cnt, my_bcomm->my_bcast_cnt, total_bcast);
    /* Forward messages until we've received all the broadcasts */
    if(tag == BC_TEARDOWN) {
        while (my_bcomm->bcast_recv_cnt + my_bcomm->my_bcast_cnt < total_bcast) {
            //recv_forward(my_bcomm, &recv_buf, NULL);
            irecv_wrapper(my_bcomm, &recv_buf, NULL);
        }
        /* If there are outstanding messages being forwarded, wait for them now */
        if(my_bcomm->fwd_send_cnt[0] > 0) {
            MPI_Waitall(my_bcomm->fwd_send_cnt[0], my_bcomm->fwd_isend_reqs[0], my_bcomm->fwd_isend_stats[0]);
            my_bcomm->fwd_send_cnt[0] = 0;
        } /* end if */
        if(my_bcomm->fwd_send_cnt[1] > 0) {
            MPI_Waitall(my_bcomm->fwd_send_cnt[1], my_bcomm->fwd_isend_reqs[1], my_bcomm->fwd_isend_stats[1]);
            my_bcomm->fwd_send_cnt[1] = 0;
        } /* end if */

        /* Retrieve the # of broadcasts we've received */
        recv_cnt = my_bcomm->bcast_recv_cnt;
    }




    /* Cancel outstanding non-blocking receive */
    MPI_Cancel(&my_bcomm->irecv_req);

    /* Release resources */
    MPI_Comm_free(&my_bcomm->my_comm);
    free(my_bcomm->fwd_isend_reqs[0]);
    free(my_bcomm->fwd_isend_reqs[1]);
    free(my_bcomm->fwd_isend_stats[0]);
    free(my_bcomm->fwd_isend_stats[1]);
    free(my_bcomm->bcast_isend_reqs);
    free(my_bcomm->bcast_isend_stats);
    free(my_bcomm->recv_buf[0]);
    free(my_bcomm->recv_buf[1]);
    free(my_bcomm->send_buf);
    free(my_bcomm->send_list);
    free(my_bcomm);

    return recv_cnt;
}

//Make decision on which proposal will win, or both wins(compatible proposals)
//A toy comparitor, return 1 for agreed, 0 for denied.
int proposal_agree(char* p1, char* p2) {
    return 1; //for test multi proposal
//    int ret = strcmp(p1, p2);
//    if(ret == 0)
//        return 1;
//    else
//        return 0;
}

//Return 0 if p1 wins, otherwise return 1.
int proposal_compete(char* p1, char* p2) {//int proposal_compete(char* p1, int rank1,  char* p2, int rank2)
//    if(p1[0] == p2[0]) {
//        return (rank1 < rank2) ? rank1 : rank2;
//    }

    if(p1[0] >= p2[0]) {
        return 1;//0
    } else
        return 0;//1
}

int iAllReduceStart(bcomm* my_bcomm, char* my_proposal, unsigned long prop_size, ID my_proposal_id) {

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    char* recv_buf = malloc(2* MSG_SIZE_MAX);
    //SN pid = 1234;//make_pid(my_bcomm);

    int recv_vote_cnt = 0;
    int recved_tag = 0;
    int decision_cnt = 0;
    //Vote votes_result = 1;
    Vote recv_vote = 1;
    int ret = -1;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

    my_bcomm->my_proposal = my_proposal;
    my_bcomm->my_own_proposal.pid = my_proposal_id;
    my_bcomm->my_own_proposal.vote =1;
    my_bcomm->my_own_proposal.votes_needed = my_bcomm->send_list_len;
    my_bcomm->my_own_proposal.votes_recved = 0;
    my_bcomm->IAR_active = 1;

    unsigned buf_len;
    if(0 != pbuf_serialize(my_bcomm->my_own_proposal.pid, my_bcomm->my_own_proposal.vote, prop_size, my_bcomm->my_proposal, my_bcomm->user_send_buf, &buf_len)) {
        printf("pbuf_serialize failed.\n");
        return -1;
    }
    bcast(my_bcomm, IAR_PROPOSAL);//IAR_PROPOSAL
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    //use send_list_len ONLY on the started, other places use proposal_sent_cnt.
    //my_bcomm->my_own_proposal.votes_recved < my_bcomm->send_list_len
    while ( my_bcomm->my_own_proposal.votes_recved < my_bcomm->send_list_len ) {// loop until decision is ready for bcast.
        MPI_Status stat_recv;
        ret = irecv_wrapper(my_bcomm, &recv_buf, &stat_recv);
        recved_tag = stat_recv.MPI_TAG;
        //if(ret == 5)
            //break;

        if(recved_tag == IAR_DECISION) {
            decision_cnt++;
        }
        if(ret != -1) {
//            printf("%s:%u - rank = %03d, starter: irecv() = %d\n", __func__, __LINE__, my_bcomm->my_rank, ret);
        }
        //ret == 3: all votes are received

        if(ret == 3 || ret == 2 || ret == 1) {//received all votes for my proposals: note that vote won't be forwarded, so they all reach the destination directly.
            recv_vote_cnt++;
            PBuf* pbuf = malloc(sizeof(PBuf));
            pbuf_deserialize(recv_buf, pbuf);
            //votes_result &= my_bcomm->vote_my_proposal_no_use;// nouse
            //vote_pool_set(my_bcomm->my_vote_pools, other_id, ) no need, it's updated by irecv.
            if(ret == 1) {
                printf("%s:%u - rank = %03d, received a proposal(ret = %d) from rank %03d, tag = %d, "
                        "pid = %d, vote = %d, I voted no due to a compete() result.\n",
                        __func__, __LINE__, my_bcomm->my_rank, ret, stat_recv.MPI_SOURCE, recved_tag,
                        pbuf->pid, pbuf->vote);
            } else {
                printf("%s:%u - rank = %03d, received a vote(ret = %d) from rank %03d, tag = %d, "
                        "pid = %d, vote = %d, recv_vote_cnt = %d, need %d in total.\n",
                        __func__, __LINE__, my_bcomm->my_rank, ret, stat_recv.MPI_SOURCE, recved_tag,
                        pbuf->pid, pbuf->vote, my_bcomm->my_own_proposal.votes_recved, my_bcomm->my_own_proposal.votes_needed);
            }

        }
        if ( ret == 0 || ret == 4) {//make_progress() == 0, received and forwarded something: proposal or decision, won't be a vote.

        }
    }
    //packing a decision: pid:vote:proposal
    //if vote is 0, proposal is empty.
    my_bcomm->IAR_active = 0;
    printf("%s:%u - rank = %03d, Starter bcasting final decision (ID:decision): %d:%d\n", __func__, __LINE__, my_bcomm->my_rank, my_proposal_id, my_bcomm->my_own_proposal.vote);
    //memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(unsigned int), &my_bcomm->my_own_proposal.vote, sizeof(Vote));?????????
    unsigned int send_buf_len;
    pbuf_serialize(my_bcomm->my_own_proposal.pid, my_bcomm->my_own_proposal.vote, prop_size, my_bcomm->my_proposal, my_bcomm->user_send_buf, &send_buf_len);
    bcast(my_bcomm, IAR_DECISION);

    //bufer_maintain_irecv(my_bcomm);
    //free(recv_buf);??
    return my_bcomm->my_own_proposal.vote;
}//end iAllReduceStart

//no_rank: the rank that votes "NO".
int test_IAllReduce_single_proposal(bcomm* my_bcomm, int starter, int no_rank) {
    bcomm_engine_t* eng = progress_engine_new(my_bcomm);
    char* my_proposal = "111";
    char* recv_buf = malloc(2 * MSG_SIZE_MAX);
    int result = -1;
    //printf("Rank %d: send_channel_cnt = %d, send_list_len = %d\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, my_bcomm->send_list_len);
    if (eng->my_bcomm->my_rank == starter) {
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        int len = strlen(my_proposal);
        //result = iAllReduceStart(my_bcomm, my_proposal, len, my_bcomm->my_rank);
        int my_proposal_id = eng->my_bcomm->my_rank;
        result = _iar_submit_proposal(eng, my_proposal, strlen(my_proposal), my_proposal_id);

    } else {
        usleep(1500);
        if (eng->my_bcomm->my_rank  == no_rank) {
            eng->my_bcomm->my_proposal = "000";
        } else {
            eng->my_bcomm->my_proposal = "111";
        }
        int tag_recv = -1;
        int ret = -1;

        MPI_Status stat_recv;
        do {
            bcomm_GEN_msg_t* recv_msg = NULL;
            make_progress_gen(eng, &recv_msg);
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            bcomm_GEN_msg_t* pickup_out = NULL;
            sleep(1);
            while(user_pickup_next(eng, &pickup_out)){
                printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
                PBuf* pbuf = malloc(sizeof(PBuf));

                pbuf_deserialize(pickup_out->data_buf, pbuf);
                //tag_recv = pbuf->
                switch (pickup_out->irecv_stat.MPI_TAG) {
                    case IAR_PROPOSAL:
                        printf("Rank %d: Received proposal: %d:%s.\n", eng->my_bcomm->my_rank, pbuf->pid, pbuf->data);
                        break;
                    case IAR_VOTE:
                        printf("Rank %d: Received vote: %d:%d.\n", eng->my_bcomm->my_rank, pbuf->pid, pbuf->vote);
                        break;
                    case IAR_DECISION:
                        printf("Rank %d: Received decision: %d:%d:%s.\n", eng->my_bcomm->my_rank, pbuf->pid, pbuf->vote, pbuf->data);
                        break;

                    default:
                        printf("Warning: Rank %d: Received unexpected msg, tag = %d.\n", eng->my_bcomm->my_rank, tag_recv);
                        break;
                }
                user_msg_done(eng, pickup_out);
                pickup_out = NULL;
                pbuf_free(pbuf);
            }


            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);


        } while (tag_recv != IAR_DECISION);
    }

    MPI_Barrier(eng->my_bcomm->my_comm);

    if(eng->my_bcomm->my_rank == starter) {
        if (result) {
            printf("\n\n =========== Proposal approved =========== \n\n");
        } else {
            printf("\n\n =========== Proposal declined =========== \n\n");
        }
    }

    engine_cleanup(eng);
    //free(recv_buf); can' free, why?
    return -1;
}

int test_IAllReduce_multi_proposal(bcomm* my_bcomm, int starter_1, int starter_2) {
    my_bcomm->IAR_active = 0;
    char* recv_buf = malloc(2 * MSG_SIZE_MAX);
    int result = -1;
    int proposal_len = 0;
    int receved_decision = 0;//# of decision received, same with the number of proposals. Not ideal, but works as a testcase for now.
    //printf("Rank %d: send_channel_cnt = %d, send_list_len = %d\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, my_bcomm->send_list_len);
    if (my_bcomm->my_rank == starter_1) {
        my_bcomm->IAR_active = 1;
        my_bcomm->my_proposal = "111";
        proposal_len = strlen(my_bcomm->my_proposal);
        printf("%s:%u - rank = %03d, starter_early launching: proposal: (%d:%s)  \n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_rank, my_bcomm->my_proposal);
        result = iAllReduceStart(my_bcomm, my_bcomm->my_proposal, proposal_len, my_bcomm->my_rank);

    } else if(my_bcomm->my_rank  == starter_2) {
        my_bcomm->IAR_active = 1;
        usleep(200); //after all started
        my_bcomm->my_proposal = "000";
        proposal_len = strlen(my_bcomm->my_proposal);
        printf("%s:%u - rank = %03d, starter_late launching: proposal: (%d:%s)  \n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_rank, my_bcomm->my_proposal);
        result = iAllReduceStart(my_bcomm, my_bcomm->my_proposal, proposal_len, my_bcomm->my_rank);
    } else {// all passive ranks.
        usleep(500);//before starter_2
        printf("%s:%u - rank = %03d, passive ranks started \n", __func__, __LINE__, my_bcomm->my_rank);
        my_bcomm->my_proposal = "111";

        int tag_recv = -1;
        int ret = -1;
        MPI_Status stat_recv;
        do {

            ret = irecv_wrapper(my_bcomm, &recv_buf, &stat_recv);
            tag_recv = stat_recv.MPI_TAG;

            if(ret == -1) {
                continue;
            }

            PBuf* pbuf = malloc(sizeof(PBuf));

            pbuf_deserialize(recv_buf, pbuf);

            //printf("%s:%u - rank = %03d, passive rank received msg, tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, tag_recv);
            switch (tag_recv) {
            case IAR_PROPOSAL:
                printf("Passive Rank %03d: Received proposal: %d:%s.\n", my_bcomm->my_rank, pbuf->pid, pbuf->data);
                break;
            case IAR_VOTE:
                printf("Passive Rank %03d: Received vote from rank %03d: %d:%d.\n", my_bcomm->my_rank, stat_recv.MPI_SOURCE, pbuf->pid, pbuf->vote);
                break;
            case IAR_DECISION:
                printf("Passive Rank %03d: Received decision from rank %03d: %d:%d:%s.\n", my_bcomm->my_rank, stat_recv.MPI_SOURCE, pbuf->pid, pbuf->vote, pbuf->data);
                receved_decision++;
                break;

            default:
                printf("Warning: Passive Rank %d: Received unexpected msg from rank %03d, tag = %d, ret = %d.\n", my_bcomm->my_rank, stat_recv.MPI_SOURCE, tag_recv, ret);
                break;
            }

            pbuf_free(pbuf);
            //usleep(600);
        } while (receved_decision < 2);
        bufer_maintain_irecv(my_bcomm);
    }
    printf("Rank %d I'm done, waiting at barrier now.\n", my_bcomm->my_rank);

    bcomm_teardown(my_bcomm, IAR_TEARDOWN);
    //MPI_Barrier(my_bcomm->my_comm);

    int my_rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    if(my_rank == starter_1 || my_rank == starter_2) {
        if (result) {
            printf("\n\n Rank %d =========== Proposal %s approved =========== \n\n", my_rank, my_bcomm->my_proposal);
        } else {
            printf("\n\n Rank %d =========== Proposal %s declined =========== \n\n", my_rank, my_bcomm->my_proposal);
        }
    }
    //free(recv_buf); can' free, why?
    return -1;
}

int anycast_benchmark(bcomm* my_bcomm, int root_rank, int cnt, int buf_size) {
    if(buf_size > MSG_SIZE_MAX) {
        printf("Message size too big. Maximum allowed is %d\n", MSG_SIZE_MAX);
        return -1;
    }

    char* buf = calloc(buf_size, sizeof(char));
    char* recv_buf = calloc(buf_size*2, sizeof(char));
    int recved_tag = 0;
    int recved_cnt = 0;
    unsigned long start = get_time_usec();
    unsigned long time_send = 0;
    unsigned long time_recv = 5;
    if(my_bcomm->my_rank == root_rank) {//send
        //load data for bcast
        my_bcomm->user_send_buf = buf;
        for(int i = 0; i < cnt; i++) {
            bcast(my_bcomm, BCAST);
        }

    } else {//recv
        MPI_Status stat;
        do{
            if(irecv_wrapper(my_bcomm, &recv_buf, &stat) == 0) {
                recved_cnt++;
            }
        } while(recved_cnt < cnt);
    }
    unsigned long end = get_time_usec();
    //MPI_Barrier(my_bcomm->my_comm);
    time_send = end - start;

    //printf("Rank %d: Anycast ran %d times, average costs %lu usec/run\n", my_bcomm->my_rank, cnt, (end - start)/cnt);
    MPI_Barrier(my_bcomm->my_comm);
    MPI_Reduce(&time_send, &time_recv, 1, MPI_UNSIGNED_LONG, MPI_MAX, root_rank, my_bcomm->my_comm);//MPI_MAX
    if(my_bcomm->my_rank == root_rank) {
        float time_avg = time_recv/cnt;
        printf("Root: Anycast ran %d times, average costs %f usec/run\n", cnt, time_avg);
    }
    return 0;
}

int native_benchmark_single_point_bcast(MPI_Comm my_comm, int root_rank, int cnt, int buf_size) {
    char* buf = calloc(buf_size, sizeof(char));
    char recv_buf[MSG_SIZE_MAX] = {'\0'};
    // native mpi bcast

    int my_rank;
    MPI_Comm_rank(my_comm, &my_rank);

    if(my_rank == root_rank) {
        //sleep(1);
        unsigned long start = get_time_usec();
        MPI_Barrier(my_comm);
        for(int i = 0; i < cnt; i++) {
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
            MPI_Bcast(buf, buf_size, MPI_CHAR, root_rank, my_comm);
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        }
        unsigned long end = get_time_usec();
        printf("Native MPI_Bcast ran %d times, average costs %lu usec/run\n", cnt, (end - start)/cnt);
    } else {
        MPI_Status stat;
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        MPI_Barrier(my_comm);
        for(int i = 0; i < cnt; i++) {
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
            MPI_Recv(recv_buf, buf_size, MPI_CHAR, root_rank, MPI_ANY_TAG, my_comm, &stat);
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
            //MPI_Irecv(recv_buf, buf_size, MPI_CHAR, root_rank, 0, my_comm, &req);
            printf("Rank %d received %d times\n", my_rank, cnt);
        }
    }
    //MPI_Barrier(my_comm);
    free(buf);
    return 0;
}

int get_prev_rank(int my_rank, int world_size) {
    return (my_rank + (world_size - 1)) % world_size;
}

int get_next_rank(int my_rank, int world_size) {
    return (my_rank + 1) % world_size;
}

int random_rank(int my_rank, int world_size) {
    int next_rank;

    do {
        next_rank = rand() % world_size;
    } while(next_rank == my_rank);

    return next_rank;
}


int hacky_sack_progress_engine(int cnt, bcomm* my_bcomm){

    bcomm_engine_t* eng = progress_engine_new(my_bcomm);

    unsigned long time_start;
    unsigned long time_end;
    unsigned long phase_1;
    int bcast_sent_cnt;
    int recv_msg_cnt = 0;
    int my_rank = my_bcomm->my_rank;
    char buf_send[32] = "";
    /* Get pointer to sending buffer */
    total_pickup = 0;
    time_start = get_time_usec();

    /* Compose message to send (in bcomm's send buffer) */
    int next_rank = get_next_rank(my_bcomm->my_rank, my_bcomm->world_size);;
            //random_rank(my_bcomm->my_rank, my_bcomm->world_size);
            //get_next_rank(my_bcomm->my_rank, my_bcomm->world_size);
            //prev_rank(my_bcomm->my_rank, my_bcomm->world_size);
    sprintf(buf_send, "%d", next_rank);

    printf("%s:%u - rank = %03d bcast rank = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, next_rank);
    /* Broadcast message */
    bcomm_GEN_msg_t* prev_rank_bc_msg = msg_new_bc(eng, buf_send, sizeof(buf_send));
    bcast_gen(eng, prev_rank_bc_msg, BCAST);
    bcast_sent_cnt = 0;
    int world_size = my_bcomm->world_size;
    bcomm_GEN_msg_t* recv_msg = NULL;
    bcomm_GEN_msg_t* pickup_out = NULL;
    int t = 0;
    while (bcast_sent_cnt < cnt) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        make_progress_gen(eng, &recv_msg);
        if(recv_msg){
            printf("%s:%u - rank = %03d make_progress_gen output msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_msg->data_buf);
            t++;
            recv_msg = NULL;
        }
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        while(user_pickup_next(eng, &pickup_out)){
            recv_msg_cnt++;
            total_pickup++;
            printf("%s:%u - rank = %03d, pickup_out msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, pickup_out->data_buf);

            int recv_rank = atoi((pickup_out->data_buf));

            user_msg_done(eng, pickup_out);

            //printf("%s:%u - rank = %03d  bcast_cnt = %d, recv_rank = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, bcast_sent_cnt, recv_rank);
            if(recv_rank == my_bcomm->my_rank){

                char buf[32] = "";
                next_rank = get_next_rank(my_bcomm->my_rank, my_bcomm->world_size);
                sprintf(buf, "%d", next_rank);
                bcomm_GEN_msg_t* next_msg_send = msg_new_bc(eng, buf, sizeof(buf));

                bcast_gen(eng, next_msg_send, BCAST);

                bcast_sent_cnt++;
                printf("%s:%u - rank = %03d  bcast_cnt = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, bcast_sent_cnt);
            }
            //msg will be freed by a utility function after bcast_gen() is done.
        }
    }

    my_rank = my_bcomm->my_rank;
    //int teardown_cnt = bcomm_teardown(my_bcomm, BCAST);

    time_end = get_time_usec();
    phase_1 = time_end - time_start;
//    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    engine_cleanup(eng);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
    printf("Rank %d reports:  Hacky sack done bcast %d times (not including initial bcasts by everyone). total pickup %d times. fwd_queued = %d, expected pickup = %d\n", my_rank,
            bcast_sent_cnt,  total_pickup, eng->fwd_queued, (bcast_sent_cnt + 1) * (world_size - 1));

    return 0;
}

int hacky_sack(int cnt, int starter, bcomm* my_bcomm) {
    char *next_rank;
    unsigned long time_start;
    unsigned long time_end;
    unsigned long phase_1;
    int bcast_cnt;
    int recv_msg_cnt;
    int my_rank;

    /* Get pointer to sending buffer */
    next_rank = my_bcomm->user_send_buf;

    time_start = get_time_usec();

    /* Compose message to send (in bcomm's send buffer) */
    *(int *)next_rank = get_prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

    /* Broadcast message */
    bcast(my_bcomm, BCAST);
    bcast_cnt = 1;
    int ret = -1;
    while (bcast_cnt < cnt) {
        char *recv_buf;

        /* Retrieve a message (in internal bcomm buffer) */
        int recved_tag = 0;
        MPI_Status stat;
        ret = irecv_wrapper(my_bcomm, &recv_buf, &stat);
        recved_tag = stat.MPI_TAG;
        if ( ret == 0 && recved_tag == BCAST) {
            int recv_rank = *(int *)recv_buf;

            if (recv_rank == my_bcomm->my_rank) {
                /* If there are outstanding messages being broadcast, wait for them now, before re-using buffer */
                if(my_bcomm->bcast_send_cnt > 0) {
                    MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
                    my_bcomm->bcast_send_cnt = 0;
                } /* end if */

                /* Compose message to send (in bcomm's send buffer) */
                *(int *)next_rank = get_prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

                /* Broadcast message */
                bcast(my_bcomm, BCAST);
                bcast_cnt++;
            }
        }
    }

    my_rank = my_bcomm->my_rank;
    recv_msg_cnt = bcomm_teardown(my_bcomm, BCAST);

    time_end = get_time_usec();
    phase_1 = time_end - time_start;

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Rank %d reports:  Hacky sack done, round # = %d . received %d times.  Phase 1 cost %lu msecs\n", my_rank,
            bcast_cnt, recv_msg_cnt, phase_1 / 1000);

    return 0;
}


int queue_test(int cnt){
    queue q;
    q.head = NULL;
    q.tail = NULL;
    q.msg_cnt = 0;
    bcomm_GEN_msg_t* new_msg = calloc(1, sizeof(bcomm_GEN_msg_t));
    for(int i = 0; i < cnt; i++){
        bcomm_GEN_msg_t* new_msg = calloc(1, sizeof(bcomm_GEN_msg_t));
        new_msg->id_debug = i;
        new_msg->fwd_done = 1;
        new_msg->pickup_done = 1;
        queue_append(&q, new_msg);
    }

    bcomm_GEN_msg_t* cur = q.head;
    printf("cur = %p\n", cur);
    while(cur){
        printf("Looping queue: msg->id_debug = %d\n", cur->id_debug);
        cur = cur->next;
    }

    cur = q.head;
    printf("cur = %p, q.cnt = %d\n", cur, q.msg_cnt);
    while(cur){
        printf("Remove element: msg->id_debug = %d\n", cur->id_debug);
        bcomm_GEN_msg_t* t = cur->next;
        queue_remove(&q, cur);
        cur = t;
    }

    cur = q.head;
    printf("cur = %p, q.cnt = %d\n", cur, q.msg_cnt);
    while(cur){
        printf("Looping queue: msg->id_debug = %d\n", cur->id_debug);
        cur = cur->next;
    }

    return 0;
}



int main(int argc, char** argv) {
    bcomm* my_bcomm;


    time_t t;
    int tmp;


    //int msg_size = atoi(argv[3]);
    //int no_rank = atoi(argv[4]);
    srand((unsigned) time(&t) + getpid());

    MPI_Init(NULL, NULL);
    if(NULL == (my_bcomm = bcomm_init(MPI_COMM_WORLD, MSG_SIZE_MAX)))
        return 0;
    //native_benchmark_single_point_bcast(MPI_COMM_WORLD, init_rank, game_cnt, msg_size);
    printf("%s:%u - rank = %03d, pid = %d\n", __func__, __LINE__, my_bcomm->my_rank, getpid());


    //queue_test(500);


    //anycast_benchmark(my_bcomm, init_rank, game_cnt, msg_size);

    int op_cnt = atoi(argv[1]);
    hacky_sack_progress_engine(op_cnt, my_bcomm);
    //int init_rank = atoi(argv[2]);
    //test_gen_bcast(my_bcomm, MSG_SIZE_MAX, init_rank, op_cnt);



    //int no_rank = atoi(argv[2]);
    //test_IAllReduce_single_proposal(my_bcomm, init_rank, no_rank);




    //    int starter_1 = atoi(argv[1]);
    //    int starter_2 = atoi(argv[2]);
    //test_IAllReduce_multi_proposal(my_bcomm, starter_1, starter_2);



    //printf("Parsing result: sample_str = %s,\n path = %s, level = %d, format = %s.\n", sample_str, file_path_out, level, format);
    MPI_Finalize();

    return 0;
}

