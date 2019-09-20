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
 * rootless_ops.c
 *
 *  Created on: Aug, 2018
 *      Author: Tonglin Li
 */

#include "rootless_ops.h"

#define PROPOSAL_POOL_SIZE 16 //maximal concurrent proposal supported
#define ISEND_CONCURRENT_MAX 128 //maximal number of concurrent and unfinished isend, used to set MPI_Request and MPI_State arrays for MPI_Waitall().

#define DEBUG_PRINT printf("%s:%u, process_id = %d\n", __func__, __LINE__, getpid());

typedef struct EngineManager {
    RLO_engine_t* head;
    RLO_engine_t* tail;
    int engine_cnt;//current active engines
    int _eng_ever_created;//engines that are ever created, used as a sn
} EngineManager;

EngineManager* Active_Engines;

EngineManager* engine_manager_new();

EngineManager* engine_manager_new(){
    EngineManager* mngr_new = calloc(1, sizeof(EngineManager));
    return mngr_new;
}


enum MSG_TAGS {//Used as a msg tag, it's a field of the msg. Class 2
    IAR_Vote // to replace IAR_VOTE in mpi_tag
};

typedef struct queue {
    RLO_msg_t* head;
    RLO_msg_t* tail;
    int msg_cnt;
}queue;

int queue_append(queue* q, RLO_msg_t* msg);
int queue_remove(queue* q, RLO_msg_t* msg);


typedef struct bcomm_token_t {
    RLO_ID req_id;
    RLO_Req_stat req_stat;
    bool completed;
    bool vote_result;
    struct bcomm_token_t* next;
} bcomm_token_t;

typedef struct BCastCommunicator bcomm;
bcomm *bcomm_init(MPI_Comm comm, size_t msg_size_max);

struct BCastCommunicator {
    /* MPI fields */
    MPI_Comm my_comm;                   /* MPI communicator to use */
    int my_rank;                        /* Local rank value */
    int world_size;                     /* # of ranks in communicator */

    /* Message fields */
    size_t msg_size_max;                /* Maximum message size */

    /* Skip ring fields */
    int my_level;                       /* Level of rank in ring */
    int last_wall;                      /* The closest rank that has higher level than rank world_size - 1 */
    int world_is_power_of_2;            /* Whether the world size is a power of 2 */

    /* Send fields */
    int send_channel_cnt;               /* # of outgoing channels from this rank */
    int send_list_len;                  /* # of outgoing ranks to send to */
    int* send_list;                     /* Array of outgoing ranks to send to */

    int bcast_send_cnt;                 /* # of outstanding non-blocking broadcast sends */
    
    /* Operation counters */
    int my_bcast_cnt;
    int bcast_recv_cnt;
    /* Request progress status*/
    bcomm_token_t* req_stat;
};

bcomm *bcomm_init(MPI_Comm comm, size_t msg_size_max);

//char DEBUG_MODE = 'O';
typedef struct {
    FILE* log_file;
    int my_rank;
} Log;
Log MY_LOG;

// Skip ring utility funcitons
int get_origin(void* buf_in);
int check_passed_origin(const bcomm* my_bcomm, int origin_rank, int to_rank);
int fwd_send_cnt(const bcomm* my_bcomm, int origin_rank, int from_rank);//return # of sends needed if forward a msg

int RLO_get_my_rank(){
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    return my_rank;
}

unsigned long RLO_get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return 1000000 * tv.tv_sec + tv.tv_usec;
}

void RLO_get_time_str(char *str_out) {
    time_t rawtime;
    struct tm * timeinfo;
    time(&rawtime);
    timeinfo = localtime(&rawtime);
    sprintf(str_out, "%d:%d:%d", timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}

int RLO_get_world_size(){
    int world_size = -1;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    return world_size;
}



int get_level(int world_size, int rank);

int proposal_state_init(RLO_proposal_state* pp_in_out, RLO_msg_t* prop_msg_in);
//Proposal pool ops

int proposalPool_proposal_add(RLO_proposal_state* pools, RLO_proposal_state* pp_in);
int proposalPool_proposal_setNeededVoteCnt(RLO_proposal_state* pools, RLO_ID k, int cnt);
int proposalPool_vote_merge(RLO_proposal_state* pools, RLO_ID k, RLO_Vote v, int* p_index_out);
int proposalPool_get(RLO_proposal_state* pools, RLO_ID k, RLO_proposal_state* result);
int proposalPool_get_index(RLO_proposal_state* pools, RLO_ID k);
int proposalPool_rm(RLO_proposal_state* pools, RLO_ID k);
int proposalPools_reset(RLO_proposal_state* pools);

/* ----------------------------------------------------------------------- */
/* ----------------- refactoring for progress engine BEGIN --------------- */

// IAR is built on top of bcast, so it's not aware of BC related state changes, only cares about proposal state change.



typedef struct isend_state isend_state;
typedef struct isend_state{
    MPI_Request req;
    MPI_Status stat;
    isend_state* prev;
    isend_state* next;
}isend_state;

typedef struct bcomm_IAR_state bcomm_IAR_state_t;

struct Proposal_state{
    RLO_ID pid; // proposal ID, default = -1;
    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
//    int proposal_sent_cnt;
    RLO_Vote vote; //accumulated vote, default = 1;
    int votes_needed; //num of votes needed to report to parent, equals to the number of sends of a proposal
    int votes_recved;
    RLO_Req_stat state; //the state of this proposal: COMPLETED, IN_PROGRESS or FAILED.
    RLO_msg_t* proposal_msg;//the last place holds a proposal, should be freed when decision is made and executed.
    RLO_msg_t* decision_msg;
}; //clear when vote result is reported.

struct bcomm_IAR_state_t {
    RLO_proposal_state prop_state;
    //enum State_IAR iar_state;
    bcomm_IAR_state_t *next, *prev;
};

struct progress_engine {
    bcomm *my_bcomm;
    int engine_id;
    //generic queues for bc
    queue queue_recv;
    queue queue_wait;//waiting for isend completion.
    queue queue_pickup; //ready for pickup
    queue queue_wait_and_pickup;//act like have both two roles above

    queue queue_iar_pending; //store received proposal msgs

    queue queue_delivery; //store things to deliver, such as approved proposals.
//    bcomm_GEN_msg_t
//        *rcv_q_head,
//        *rcv_q_tail; // Used by BC, proposal, vote, decision

    unsigned int bc_incomplete;
    unsigned int recved_bcast_cnt;
    unsigned int sent_bcast_cnt;

    //=================   IAR   =================
    void* user_iar_ctx;
    RLO_msg_t
        *iar_prop_rcv_q_head,
        *Iar_prop_rcv_q_tail;
    bcomm_IAR_state_t
        *prop_state_q_head,
        *prop_state_q_tail;

    RLO_msg_t // dedicated buff for receiving votes by mpi_tag.
        *iar_vote_recv_q_head,
        *iar_vote_recv_q_tail;
    RLO_msg_t
        *iar_decision_recv_q_head,
        *iar_decision_recv_q_tail;
    isend_state
        *iar_send_stats_head,
        *iar_send_stats_tail;
    unsigned int iar_incomplete;
    RLO_Vote vote_my_proposal_no_use;          /* Used only by an proposal-active rank. 1 for agree, 0 for decline. Accumulate votes for a proposal that I just submitted. */
    RLO_proposal_state my_own_proposal;      /* Set only when I'm a IAR starter, maintain status for my own proposal */
    RLO_proposal_state proposal_state_pool[PROPOSAL_POOL_SIZE];        /* To support multiple proposals, use a vote pool for each proposal. Use linked list if concurrent proposal number is large. */
    char* my_proposal;

    iar_cb_func_t prop_judgement_cb; //provided by the user, used to judge if agree with a proposal
    void *app_ctx;
    iar_cb_func_t proposal_action; //if a proposal is approved, what to do with it.

    //debug variables
    int fwd_queued;
    RLO_engine_t* prev;
    RLO_engine_t* next;
};

int RLO_get_eng_rank(RLO_engine_t* eng){
    return eng->my_bcomm->my_rank;
}

int msg_wait(RLO_engine_t* eng, RLO_msg_t* msg_in);

int _test_ircecv_completed(RLO_engine_t* eng, RLO_msg_t* msg_buf);

//int _gen_bc_msg_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);

//Generic function, to (re)post a irecv. Used by BC, IAR and all other places that need a buff to recv.
int _post_irecv_gen(RLO_engine_t* eng, RLO_msg_t* recv_msg_buf, enum RLO_COMM_TAGS rcv_tag);

//Progress engine queue process functions
int _wait_and_pickup_queue_process(RLO_engine_t* en, RLO_msg_t* msg);
int _wait_only_queue_cleanup(RLO_engine_t* eng);

// actions for proposals, votes and decisions. Called in make_progress_gen() loop.
int _iar_proposal_handler(RLO_engine_t* eng, RLO_msg_t* recv_msg_buf_in);
int _iar_vote_handler(RLO_engine_t* eng, RLO_msg_t* recv_msg_buf_in);
int _iar_decision_handler(RLO_engine_t* eng, RLO_msg_t* recv_msg_buf_in);

int _vote_back(RLO_engine_t* eng, RLO_proposal_state* ps, RLO_Vote vote);
int _iar_decision_bcast(RLO_engine_t* eng, RLO_ID my_proposal_id, RLO_Vote decision);
int _vote_merge(RLO_engine_t* eng, int pid, RLO_Vote vote_in, RLO_proposal_state* ps_out);

RLO_msg_t* _find_proposal_msg(RLO_engine_t* eng, RLO_ID pid);

//Type for user callback functions
typedef struct proposals_ctx{

}proposals_ctx;

int _bc_forward(RLO_engine_t* eng, RLO_msg_t* msg);//new version

//For irecv and other generic use
RLO_msg_t* RLO_msg_new_generic(RLO_engine_t* eng) {
    //DEBUG_PRINT
    RLO_msg_t* new_msg = calloc(1, sizeof(RLO_msg_t));
    //printf("%s:%u - rank = %03d: new_msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, new_msg);
    new_msg->msg_usr.pid = -1;
    new_msg->msg_usr.type = -1;
    new_msg->msg_usr.vote = -1;
    new_msg->msg_usr.time_stamp = 0;
    new_msg->data_buf = new_msg->msg_usr.buf + sizeof(int);

    new_msg->bc_isend_reqs = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Request));

    new_msg->bc_isend_stats = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Status));
    //DEBUG_PRINT
    new_msg->pickup_done = 0;
    //DEBUG_PRINT
    new_msg->bc_init = 0;//by default 0, when created to be a recv buf.
    //DEBUG_PRINT
    new_msg->prev = NULL;
    //DEBUG_PRINT
    new_msg->next = NULL;
    //DEBUG_PRINT
    new_msg->send_cnt = 0;
    //DEBUG_PRINT
    new_msg->msg_usr.data = new_msg->msg_usr.buf + sizeof(int);// == msg->data_buf.
    // Set msg origin
    //DEBUG_PRINT
    *(int*)(new_msg->msg_usr.buf) = eng->my_bcomm->my_rank;
    //DEBUG_PRINT
    return new_msg;
}

RLO_msg_t* RLO_msg_new_bc(RLO_engine_t* eng, void* buf_in, int send_size) {
    RLO_msg_t* new_msg = RLO_msg_new_generic(eng);

    memcpy(new_msg->data_buf, buf_in, send_size);
    new_msg->bc_init = 1;//by default. set to 0 when created to be a recv buf.
    return new_msg;
}

int RLO_msg_test_isends(RLO_engine_t* eng, RLO_msg_t* msg_in) {
    assert(eng);
    assert(msg_in);
    int completed = 0;
    MPI_Testall(msg_in->send_cnt, msg_in->bc_isend_reqs, &completed, msg_in->bc_isend_stats);
    return completed;
}

int msg_wait(RLO_engine_t* eng, RLO_msg_t* msg_in) {
    return MPI_Waitall(eng->my_bcomm->send_list_len, msg_in->bc_isend_reqs, msg_in->bc_isend_stats);
}

int RLO_msg_free(RLO_msg_t* msg_in) {
    if(msg_in->bc_isend_reqs)
        free(msg_in->bc_isend_reqs);

    if(msg_in->bc_isend_stats)
        free(msg_in->bc_isend_stats);

    free(msg_in);
    return 0;
}
int _queue_debug_print(queue* q){

    return q->msg_cnt;
}
int queue_append(queue* q, RLO_msg_t* msg){
    assert(q);
    assert(msg);
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if(q->head == q->tail){
        if(!q->head){ // add as the 1st msg
            msg->prev = NULL;
            msg->next = NULL;
            q->head = msg;
            q->tail = msg;
        } else {//add as 2nd msg
            msg->prev = q->tail;
            msg->next = NULL;
            q->head->next = msg;
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
int queue_remove(queue* q, RLO_msg_t* msg){
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

int engine_append(EngineManager* q, RLO_engine_t* eng){
    assert(q);
    assert(eng);
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    if(q->head == q->tail){
        if(!q->head){ // add as the 1st msg
            eng->prev = NULL;
            eng->next = NULL;
            q->head = eng;
            q->tail = eng;
        } else {//add as 2nd msg
            eng->prev = q->tail;
            eng->next = NULL;
            q->head->next = eng;
            q->tail = eng;
        }
    } else {
        eng->prev = q->tail;
        eng->next = NULL;
        q->tail->next = eng;
        q->tail = eng;
    }
    q->engine_cnt++;
    return 0;
}

//assume msg must be in the queue.
int engine_remove(EngineManager* q, RLO_engine_t* eng){
    assert(q);
    assert(eng);
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
        if(eng == q->head){//remove head msg
            q->head = q->head->next;
            q->head->prev = NULL;
        } else if (eng == q->tail){// non-head msg
            q->tail = q->tail->prev;
            q->tail->next = NULL;
        } else{ // in the middle of queue
            eng->prev->next = eng->next;
            eng->next->prev = eng->prev;
        }
        ret = 1;
    }

    eng->prev = NULL;
    eng->next = NULL;

    q->engine_cnt--;
    return ret;
}
RLO_engine_t* RLO_progress_engine_new(MPI_Comm mpi_comm, size_t msg_size_max, void* approv_cb_func, void* app_ctx, void* app_proposal_action){
    RLO_engine_t* eng = calloc(1, sizeof(RLO_engine_t));
    DEBUG_PRINT
    eng->my_bcomm = bcomm_init(mpi_comm, msg_size_max);
    assert(eng->my_bcomm);
    DEBUG_PRINT
    eng->prop_judgement_cb = approv_cb_func;
    eng->proposal_action = app_proposal_action;
    eng->app_ctx = app_ctx;

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
    //DEBUG_PRINT
    RLO_proposal_state new_prop_state;
    proposal_state_init(&new_prop_state, NULL);
    //DEBUG_PRINT
    eng->my_own_proposal = new_prop_state;
    //DEBUG_PRINT
    RLO_msg_t* msg_irecv_init = RLO_msg_new_generic(eng);// DO NOT free this msg, it's freed within the framework.

    eng->fwd_queued = 0;
//    DEBUG_PRINT
    proposalPools_reset(eng->proposal_state_pool);
    //DEBUG_PRINT
    _post_irecv_gen(eng, msg_irecv_init, RLO_ANY_TAG);
    queue_append(&(eng->queue_recv), msg_irecv_init);
    eng->next = NULL;

    if(!Active_Engines){
        Active_Engines = engine_manager_new();
    }

    engine_append(Active_Engines, eng);
    Active_Engines->_eng_ever_created++;
    eng->engine_id = Active_Engines->_eng_ever_created;
    printf("%s:%u, pid = %d, engine_cnt = %d, engine_id = %d\n", __func__, __LINE__, getpid(), Active_Engines->engine_cnt, eng->engine_id);
    return eng;
}

int RLO_get_engine_id(RLO_engine_t* eng){
    assert(eng);
    return eng->engine_id;
}
MPI_Comm RLO_get_my_comm(RLO_engine_t* eng){
    assert(eng);
    return eng->my_bcomm->my_comm;
}
void progress_engine_free(RLO_engine_t* eng){
    assert(eng);
    free(eng);
}
//Turn the gear. Output a handle(recv_msgs_out ) to the received msg, for sampling purpose only. User should use pickup_next() to get msg.

int RLO_make_progress() {
    assert(Active_Engines);
    //DEBUG_PRINT
    RLO_engine_t* e = Active_Engines->head;
    if(!e)
        return -1;

    while(e){
        //DEBUG_PRINT
        RLO_make_progress_gen(e, NULL);
        e = e->next;
    }
    //DEBUG_PRINT
    return 0;
}

int RLO_make_progress_gen(RLO_engine_t* eng, RLO_msg_t** recv_msg_out) {

    //========================== My active proposal state update==========================
    if(eng->my_own_proposal.state != RLO_COMPLETED && eng->my_own_proposal.state != RLO_INVALID){// if I have an active proposal
        //DEBUG_PRINT
        //printf("%s:%u - rank = %03d: pid = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.pid);
        if(eng->my_own_proposal.decision_msg){
            //printf("%s:%u - rank = %03d: decision_msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.decision_msg);
            DEBUG_PRINT
            if(RLO_msg_test_isends(eng, eng->my_own_proposal.decision_msg)){
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                DEBUG_PRINT
                eng->my_own_proposal.state = RLO_COMPLETED;
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                RLO_msg_free(eng->my_own_proposal.decision_msg);
                eng->my_own_proposal.decision_msg = NULL; //freed after pickup.
            }
        }
    }
    //DEBUG_PRINT
    //========================== Bcast msg handling ==========================
    RLO_msg_t* cur_bc_rcv_buf = eng->queue_recv.head;//eng->rcv_q_head;
    while(cur_bc_rcv_buf) {//receive and repost with tag = BCAST

        RLO_msg_t* msg_tmp = cur_bc_rcv_buf->next;

        if(_test_ircecv_completed(eng, cur_bc_rcv_buf)){//irecv complete.
            int recv_tag = cur_bc_rcv_buf->irecv_stat.MPI_TAG;
            //printf("%s:%u - rank = %03d, recv_tag = %d, src = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_tag, cur_bc_rcv_buf->irecv_stat.MPI_SOURCE);
            queue_remove(&(eng->queue_recv), cur_bc_rcv_buf);
            RLO_msg_t* msg_new_recv = RLO_msg_new_generic(eng);
            _post_irecv_gen(eng, msg_new_recv, RLO_ANY_TAG);
            queue_append(&(eng->queue_recv), msg_new_recv);

            switch(recv_tag){
                case RLO_BCAST: {
                    //DEBUG_PRINT
                    eng->recved_bcast_cnt++;
                    _bc_forward(eng, cur_bc_rcv_buf);
                    if(recv_msg_out)
                        *recv_msg_out = cur_bc_rcv_buf;
                    break;
                }

                case RLO_IAR_PROPOSAL: {
                    DEBUG_PRINT
                    //processed by a callback function, not visible to the users
                    //do not increase eng->recved_bcast_cnt
                    _iar_proposal_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                case RLO_IAR_VOTE: {
                    DEBUG_PRINT
//                    PBuf* t = NULL;
//                    pbuf_deserialize(cur_bc_rcv_buf, &t);
//                    pbuf_free(t);
                    _iar_vote_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                case RLO_IAR_DECISION: {
                    eng->recved_bcast_cnt++;
                    DEBUG_PRINT
                    // remove corresponding proposal state
                    int ret = _iar_decision_handler(eng, cur_bc_rcv_buf);
                    //assert(ret != -1); NO need of this: if a proposal was denied here, the local won't store it for later checking.
                    //msg logic is same as BCAST, and will always end up being picked up.
                    _bc_forward(eng, cur_bc_rcv_buf);//queue ops happen here

                    if(recv_msg_out)
                        *recv_msg_out = cur_bc_rcv_buf;
                    break;
                }

                default: {
                    printf("%s:%u - rank = %03d: received a msg with unknown tag: %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_tag);
                    break;
                }
            }
        }
        cur_bc_rcv_buf = msg_tmp;//move cursor
    }//loop through bc recv queue

    //============================ BC Wait queue processing =======================
    RLO_msg_t* cur_wait_pickup_msg = eng->queue_wait_and_pickup.head;
    while(cur_wait_pickup_msg){
        RLO_msg_t* msg_t = cur_wait_pickup_msg->next;
        _wait_and_pickup_queue_process(eng, cur_wait_pickup_msg);
        cur_wait_pickup_msg = msg_t;
    }

    //================= Cleanup wait_only and pickup_only queues ==================
    //clean up all done msgs in the wait_only queue.
    _wait_only_queue_cleanup(eng);
    //DEBUG_PRINT
    //clean up pickup_only queue is done by user_pickup_next().

    return -1;
}

int _test_ircecv_completed(RLO_engine_t* eng, RLO_msg_t* msg_buf) {
    if(!msg_buf)
        return 0;
    int completed = 0;
    MPI_Test(&(msg_buf->irecv_req), &completed, &(msg_buf->irecv_stat));

    return completed;
}

int _post_irecv_gen(RLO_engine_t* eng, RLO_msg_t* msg_buf_in_out, enum RLO_COMM_TAGS rcv_tag) {
    if(rcv_tag == RLO_ANY_TAG)
        rcv_tag = MPI_ANY_TAG;
    msg_buf_in_out->post_irecv_type =  rcv_tag;
    int ret = MPI_Irecv(msg_buf_in_out->msg_usr.buf, eng->my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, MPI_ANY_SOURCE, rcv_tag, eng->my_bcomm->my_comm, &(msg_buf_in_out->irecv_req));
    return ret;
}

int _proposal_pickup_next(){
    return -1;
}

typedef struct application_context{

}App_ctx;

int _iar_proposal_handler(RLO_engine_t* eng, RLO_msg_t* recv_msg_buf_in) {
    if (!eng || !recv_msg_buf_in)
        return -1;

    PBuf* pbuf = NULL;//malloc(sizeof(PBuf));

    int origin = get_origin(recv_msg_buf_in->msg_usr.buf);

    pbuf_deserialize(recv_msg_buf_in->data_buf, &pbuf);
//    printf("%s:%u - rank = %03d, received proposal, pid = %d, data_len = %lu\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, pbuf->pid, pbuf->data_len);
    //add a state to waiting_votes queue.
    RLO_proposal_state* new_prop_state = malloc(sizeof(RLO_proposal_state));
    //need to read pid from msg and fill in state.
    proposal_state_init(new_prop_state, recv_msg_buf_in);

    new_prop_state->proposal_msg = recv_msg_buf_in;
    new_prop_state->pid = pbuf->pid;
    new_prop_state->recv_proposal_from = recv_msg_buf_in->irecv_stat.MPI_SOURCE;
    new_prop_state->state = RLO_IN_PROGRESS;

//    printf("%s:%u - rank = %03d: received a proposal from rank %d: %p\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, recv_msg_buf_in->irecv_stat.MPI_SOURCE, recv_msg_buf_in);
    if(pbuf->pid == eng->my_own_proposal.pid){
        printf("%s:%u - rank = %03d: received a proposal with my own pid: something went wrong...\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        return -1;
    }else{
        new_prop_state->votes_needed = fwd_send_cnt(eng->my_bcomm, origin, recv_msg_buf_in->irecv_stat.MPI_SOURCE);//votes needed;
        //printf("%s:%u - rank = %03d: need %d votes. \n", __func__, __LINE__, eng->my_bcomm->my_rank, new_prop_state->votes_needed);
    }

    recv_msg_buf_in->prop_state = new_prop_state;
    int judgment = (eng->prop_judgement_cb)(pbuf->data, eng->app_ctx);//received proposal and my proposal
//    printf("%s:%u - rank = %03d, received proposal, pid = %d, prop_judgement_cb() = %d\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, pbuf->pid, judgment);
    switch (judgment) {
        case 0: {    // decline received proposal
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            //queue_append(&(eng->queue_iar_pending), recv_msg_buf_in);
            _vote_back(eng, new_prop_state, 0);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            break;
        }
        case 1: {    // Approve received proposal, others' win. either mine is empty or declined?
                //Add msg to queue_iar_pending in _bc_forward().

            int fwd_cnt = fwd_send_cnt(eng->my_bcomm, origin, recv_msg_buf_in->irecv_stat.MPI_SOURCE);
//            printf("%s:%u - rank = %03d: eng->queue_iar_pending.head = %p, queueing a proposal: %p, fwd_cnt = %d\n",
//                    __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_iar_pending.head, recv_msg_buf_in, fwd_cnt);

            if (_bc_forward(eng, recv_msg_buf_in) == 0) {//leaf node, no need to fwd.
                _vote_back(eng, new_prop_state, 1);
            }
            //else: non-leaf node, wait for children's votes.
            break;
        }
        default:
            printf("%s:%u - rank = %03d: unknown judgment received: %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, judgment);
            break;
    }
    pbuf_free(pbuf);
    return 0;
}

int _vote_back(RLO_engine_t* eng, RLO_proposal_state* ps, RLO_Vote vote){
    //printf("%s:%u - rank = %03d, vote back to rank %d, for pid = %d, vote = %d.\n", __func__, __LINE__,eng->my_bcomm->my_rank, ps->recv_proposal_from, ps->pid, vote);
    size_t send_len = 0;
    void* send_buf = NULL;

    //memcpy(send_buf, &(eng->my_bcomm->my_rank), sizeof(int));
//RLO_ID pid_in, RLO_Vote vote, RLO_time_stamp time_stamp, size_t data_len_in, void* data_in, void** buf_out, size_t* buf_len_out


    pbuf_vote_serialize(eng->my_bcomm->my_rank, ps->pid, vote, &send_buf, &send_len);
    //pbuf_serialize(ps->pid, vote, ts, d_len, NULL, &(send_buf + sizeof(int)), &send_len);
    //old send_len = eng->my_bcomm->msg_size_max + 1
    MPI_Send(send_buf, send_len, MPI_CHAR, ps->recv_proposal_from,
            RLO_IAR_VOTE, eng->my_bcomm->my_comm);
//    MPI_Request req;
//    MPI_Isend(send_buf, eng->my_bcomm->msg_size_max + 1, MPI_CHAR, ps->recv_proposal_from,
//            IAR_VOTE, eng->my_bcomm->my_comm, &req);
//    DEBUG_PRINT
    return send_len;
}

int _iar_vote_handler(RLO_engine_t* eng, RLO_msg_t* msg_buf) {
    if (!eng || !msg_buf)
        return -1;

    //update proposal_state_queue
    //decide if all necessary votes are received, then vote back
    PBuf* vote_buf = NULL;

    pbuf_deserialize(msg_buf->data_buf, &vote_buf);        //votes have same format as all other msgs

//    printf("%s:%u - rank = %03d: received a vote = %d for pid = %d\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, vote_buf->vote, vote_buf->pid);

    if (vote_buf->pid == eng->my_own_proposal.pid) { //votes for my proposal
//        printf("%s:%u - rank = %03d, received a vote from rank %03d for my proposal, vote = %d.\n", __func__, __LINE__,
//                eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE, vote_buf->vote);
        eng->my_own_proposal.votes_recved++;
        eng->my_own_proposal.vote &= vote_buf->vote; //*(Vote*)(vote_buf->data);
//        printf("%s:%u - rank = %03d, "
//                "received a vote from rank %03d for my proposal, vote = %d, "
//                "received %d votes, needed %d votes.\n", __func__, __LINE__,
//                eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE,
//                vote_buf->vote, eng->my_own_proposal.votes_recved,
//                eng->my_own_proposal.votes_needed);
        if (eng->my_own_proposal.votes_recved == eng->my_own_proposal.votes_needed) { //all done, bcast decision.
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

            if(eng->my_own_proposal.vote){
                //printf("%s:%u - rank = %03d, app_ctx = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->app_ctx);

                eng->my_own_proposal.vote = (eng->prop_judgement_cb)(eng->my_proposal, eng->app_ctx);
                //printf("%s:%u - rank = %03d, prop_judgement_cb() = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.vote);
            }
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            _iar_decision_bcast(eng, eng->my_own_proposal.pid, eng->my_own_proposal.vote);
            pbuf_free(vote_buf);
            return 0;
        } else { // need more votes for my decision, continue to irecv.
            pbuf_free(vote_buf);
            return 0;
        }

    } else { //Votes for proposals in the state queue
//        printf("%s:%u - rank = %03d, received a vote from rank %03d for other's proposal, vote = %d.\n",
//                __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE, vote_buf->vote);
        RLO_proposal_state* ps_result = calloc(1, sizeof(RLO_proposal_state));
        int ret = _vote_merge(eng, vote_buf->pid, vote_buf->vote, ps_result);
        //printf("%s:%u - rank = %03d, merged vote = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, ps_result.vote);
        //int ret = proposalPool_vote_merge(eng->proposal_state_pool, vote_buf->pid, vote_buf->vote, &p_index);

        if (ret < 0) {
            printf("Function %s:%u - rank %03d: can't merge vote, proposal not exists, pid = %d \n", __func__, __LINE__,
                    eng->my_bcomm->my_rank, vote_buf->pid);
            pbuf_free(vote_buf);
            return -1;
        } else { // Find proposal, merge completed.
            if (ret == 1) { //done collecting votes, vote back
//                printf("%s:%u - rank = %03d: done collecting votes, vote back = %d for pid = %d, vote_buf pid = %d\n",
//                        __func__, __LINE__, eng->my_bcomm->my_rank, ps_result.vote, ps_result.pid, vote_buf->pid);
                _vote_back(eng, ps_result, ps_result->vote);
                pbuf_free(vote_buf);
            } else {
//                printf("%s:%u - rank = %03d: merging done, waiting more votes for pid = %d, "
//                        "current vote = %d. received %d votes, needed %d.\n", __func__, __LINE__,
//                        eng->my_bcomm->my_rank, ps_result.pid, ps_result.vote, ps_result.votes_recved, ps_result.votes_needed);
            }
        }
    }
    return 0;
}

int _iar_decision_handler(RLO_engine_t* eng, RLO_msg_t* msg_buf_in) {
    if(!eng || !msg_buf_in){
        DEBUG_PRINT
        return -1;
    }

    //update proposal_state_queue
    PBuf* decision_buf = NULL;//malloc(sizeof(PBuf));
    pbuf_deserialize(msg_buf_in->data_buf, &decision_buf);
    //printf("%s: %d: rank = %03d, received a decision: %p = [%d:%d], prop_state = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf_in, decision_buf->pid, decision_buf->vote, msg_buf_in->prop_state);
    printf("%s:%u - rank = %03d: received a decision! pid = %d, vote = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_buf->pid, decision_buf->vote);
    RLO_msg_t* proposal_msg = _find_proposal_msg(eng, decision_buf->pid);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    //I don't have this proposal, but received a decision about it. (could only be 0)
    // Don't need to forward, but need pickup.

    if(!proposal_msg){//matching proposal not found.
        DEBUG_PRINT
        printf("%s:%u - rank = %03d, pid = %d not found\n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_buf->pid);
        queue_append(&(eng->queue_pickup), msg_buf_in);
        return -1;
    }

    if(decision_buf->vote == 0){//proposal canceled
        //no need to append to pickup_q, since pickup is for app use only.
        //printf("%s:%u - rank = %03d: received decision: proposal canceled: pid = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_buf->pid);
        queue_remove(&(eng->queue_iar_pending), proposal_msg);

        RLO_msg_free(proposal_msg);

    } else {//proposal approved
        //execute proposal: a callback function
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        if(eng->proposal_action)
            (eng->proposal_action)(proposal_msg->data_buf, eng->app_ctx);
        DEBUG_PRINT
        proposal_msg->prop_state->state = RLO_COMPLETED;

        queue_remove(&(eng->queue_iar_pending), proposal_msg);
        proposal_msg->fwd_done = 1;
        proposal_msg->pickup_done = 0;
        proposal_msg->prop_state->vote = 1;
        proposal_msg->prop_state->state = RLO_COMPLETED;

        //DO NOT change this line: this is meant to change user msg type to decision, since irecv_stat.MPI_TAG is used to set usr_msg->type.
        proposal_msg->irecv_stat.MPI_TAG = msg_buf_in->irecv_stat.MPI_TAG; //assign to decision type
        queue_append(&(eng->queue_pickup), proposal_msg);
        //RLO_msg_free(proposal_msg);
        //done using proposal msg.
    }

    //give user a notification
    //msg_buf_in->fwd_done = 1;//set for so pickup queue can free it eventually.
    //queue_append(&(eng->queue_pickup), msg_buf_in);

    pbuf_free(decision_buf);

    return 0;
}

int proposal_succeeded(RLO_engine_t* eng){
    RLO_make_progress();

    return (eng->my_own_proposal.votes_needed == eng->my_own_proposal.votes_recved
            && eng->my_own_proposal.vote != 0);
    //succeed, fail, incomplete
}

int RLO_check_my_proposal_state(RLO_engine_t* eng, int pid){
    assert(eng);
    //DEBUG_PRINT
    RLO_make_progress();
    //DEBUG_PRINT
    return eng->my_own_proposal.state;
}



int RLO_submit_proposal(RLO_engine_t* eng, char* proposal, unsigned long prop_size, RLO_ID my_proposal_id){

    eng->my_own_proposal.pid = my_proposal_id;
    eng->my_own_proposal.proposal_msg = NULL;
    eng->my_own_proposal.vote = 1;
    eng->my_own_proposal.votes_needed = eng->my_bcomm->send_list_len;
    eng->my_own_proposal.votes_recved = 0;
    eng->my_own_proposal.decision_msg = NULL;

    eng->my_proposal = proposal;//used in _iar_vote_handler() for self judgment.

    void* proposal_send_buf = NULL;//calloc(1, RLO_MSG_SIZE_MAX);
    size_t buf_len;
    RLO_time_stamp time = RLO_get_time_usec();
    if(0 != pbuf_serialize(my_proposal_id, 1, time, prop_size, (void*)proposal, &proposal_send_buf, &buf_len)) {
        printf("pbuf_serialize failed.\n");
        return -1;
    }

//    PBuf* tmp = NULL;//calloc(1, sizeof(PBuf));
//    pbuf_deserialize(proposal_send_buf , &tmp);
//    printf("%s:%u - rank = %03d: Verifying pbuf_deserialize(): tmp pid = %d, should be %d, data_len = %lu, should be %lu\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, tmp->pid, my_proposal_id, tmp->data_len, prop_size);

    RLO_msg_t* proposal_msg = RLO_msg_new_bc(eng, proposal_send_buf, buf_len);

    eng->my_own_proposal.state = RLO_IN_PROGRESS;
    eng->my_own_proposal.proposal_msg = proposal_msg;

    RLO_bcast_gen(eng, proposal_msg, RLO_IAR_PROPOSAL);

    RLO_make_progress();

    if(eng->my_own_proposal.state == RLO_COMPLETED)
        return eng->my_own_proposal.vote;//result
    else
        return -1;// not complete
}

int _iar_decision_bcast(RLO_engine_t* eng, RLO_ID my_proposal_id, RLO_Vote decision){
    size_t send_len = 0;
    void* decision_send_buf = NULL;
    char*  debug_info = "IAR_DEC";
    printf("%s:%u - rank = %03d: packing decision: pid = %d, decision = %d \n", __func__, __LINE__,
            eng->my_bcomm->my_rank, my_proposal_id, decision);
    pbuf_serialize(my_proposal_id, decision, 0, strlen(debug_info) + 1, debug_info, &decision_send_buf, &send_len);
    PBuf* b = NULL;//calloc(1, sizeof(PBuf));
    pbuf_deserialize(decision_send_buf, &b);
    printf("%s:%u - rank = %03d: checking decision pack: pid = %d, decision = %d \n", __func__, __LINE__,
            eng->my_bcomm->my_rank, b->pid, b->vote);
    RLO_msg_t* decision_msg = RLO_msg_new_bc(eng, decision_send_buf, 128);
    RLO_bcast_gen(eng, decision_msg, RLO_IAR_DECISION);
    eng->my_own_proposal.decision_msg = decision_msg;
    return -1;
}

// A msg converter, return a user_msg which shares the same pointer with it's gen_msg_in
RLO_user_msg* _user_msg_mock(RLO_msg_t* gen_msg_in){
    assert(gen_msg_in);
    RLO_user_msg* msg_out = (RLO_user_msg*)gen_msg_in;
    msg_out->type = gen_msg_in->irecv_stat.MPI_TAG;
    //printf("%s:%u, msg_out = %p, type = %d, msg_out.data = %p, data = [%s]\n", __func__, __LINE__, msg_out, msg_out->type, msg_out->data, msg_out->data);


        //printf("%s:%u, pid = %d, verifying with pbuf:  timestamp = %lu, data = [%s], len = %zu\n", __func__, __LINE__, b->pid, b->time_stamp, b->data, b->data_len);
    if(msg_out->type == RLO_IAR_DECISION){

        //printf("%s:%u, pid = %d, before assignment:  timestamp = %lu, data = [%s], len = %zu\n", __func__, __LINE__, msg_out->pid, msg_out->time_stamp, msg_out->data, msg_out->data_len);
//        msg_out->pid = *((RLO_ID*)(gen_msg_in->data_buf));
//        msg_out->vote = *((RLO_Vote*)((gen_msg_in->data_buf) + sizeof(RLO_ID)));
//        msg_out->time_stamp = *((RLO_Vote*)((gen_msg_in->data_buf) + sizeof(RLO_ID)) + sizeof(RLO_time_stamp));
//        msg_out->data_len =  *((size_t*)((gen_msg_in->data_buf) + sizeof(RLO_ID) + sizeof(RLO_Vote)) + sizeof(RLO_time_stamp));
//        msg_out->data = gen_msg_in->data_buf + sizeof(RLO_ID) + sizeof(RLO_Vote) + + sizeof(RLO_time_stamp) + sizeof(size_t);

        PBuf* b = NULL;//calloc(1, sizeof(PBuf));
        pbuf_deserialize(gen_msg_in->data_buf, &b);//b->data is just a pointer that points to a propo_buf from original proposal.
        msg_out->pid = b->pid;
        msg_out->vote = b->vote;
        msg_out->time_stamp = b->time_stamp;
        msg_out->data_len = b->data_len;
        msg_out->data = gen_msg_in->data_buf + sizeof(RLO_ID) + sizeof(RLO_Vote) + sizeof(RLO_time_stamp) + sizeof(size_t);
        pbuf_free(b);
        //b->data;
        // TODO: don't free here right now as b->data is needed outside, but need to be cleared out.
        //printf("%s:%u, pid = %d, after assignment: timestamp = %lu, data = [%s], len = %zu\n", __func__, __LINE__, msg_out->pid, msg_out->time_stamp, msg_out->data, msg_out->data_len);
    }
    return msg_out;
}

// Called by the application/user, pickup a msg from the head of the queue.
// Assuming the msg will be copied and stay safe, and will be unlinked from pickup_queue.
// The user should free msg_out when it's done.
// NOTE: if this function is called in a thread different from the progress_engine thread, there will be a thread safe issue.
int RLO_user_pickup_next(RLO_engine_t* eng, RLO_user_msg** msg_out) {
    assert(eng);
    RLO_msg_t* msg = eng->queue_wait_and_pickup.head;
    if (msg) {        //wait_and_pickup empty
        DEBUG_PRINT
        while (msg) {
            RLO_msg_t* msg_t = msg->next;

            if (!msg->pickup_done) { //find a unread msg, mark read, move to wait_fwd queue
                queue_remove(&(eng->queue_wait_and_pickup), msg);
                // mark pickup_done in user_msg_done()
                queue_append(&(eng->queue_wait), msg);
                //printf("%s:%u - rank = %03d, buf = [%s], data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg->msg_usr.buf, msg->msg_usr.data);
                *msg_out = _user_msg_mock(msg);

                return 1;
            }
            msg = msg_t;
        }
        //no msg match in this queue. go with next queue.
    }

    msg = eng->queue_pickup.head;

    if (!(eng->queue_pickup.head)) {
        DEBUG_PRINT
        return 0;
    } else { //not empty, find the first available in queue_pickup
        printf("%s:%u - rank = %03d, pickup_queue_cnt = %d\n",
                __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_pickup.msg_cnt);
        while (msg) {
            RLO_msg_t* msg_t = msg->next;
            DEBUG_PRINT
            if (!msg->pickup_done) { //return a msg
                queue_remove(&(eng->queue_pickup), msg);
                printf("%s:%u - rank = %03d, return a message = %d\n",
                        __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_pickup.msg_cnt);
                // mark pickup_done and free the msg in user_msg_done()
                *msg_out = _user_msg_mock(msg);
                printf("%s:%u - rank = %03d, return a message for pickup, pid = %d\n",
                        __func__, __LINE__, eng->my_bcomm->my_rank, (*msg_out)->pid);
                return 1;
            }

            msg = msg_t; // next
        }
    }
    return 0;
}

int RLO_user_msg_recycle(RLO_engine_t* eng, RLO_user_msg* msg_in){
    assert(eng && msg_in);
    RLO_msg_t* msg = (RLO_msg_t*) msg_in;
    msg->pickup_done = 1;
    if(msg->fwd_done){
        RLO_msg_free(msg);
        return 1;
    }

    //still in wait queue
    return 0;
}

// Loop through all msgs in the queue, test if all isends are done.
int _wait_and_pickup_queue_process(RLO_engine_t* eng, RLO_msg_t* wait_and_pickup_msg){
    int ret = -1;
    if(RLO_msg_test_isends(eng, wait_and_pickup_msg)){//test if all isends are done
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

int _wait_only_queue_cleanup(RLO_engine_t* eng){
    int ret = -1;
    RLO_msg_t* cur_wait_only_msg = eng->queue_wait.head;
    while(cur_wait_only_msg){
        RLO_msg_t* msg_t = cur_wait_only_msg->next;

        if(RLO_msg_test_isends(eng, cur_wait_only_msg)){
            cur_wait_only_msg->fwd_done = 1;
            queue_remove(&(eng->queue_wait), cur_wait_only_msg);

            if(cur_wait_only_msg->send_type == RLO_BCAST){//cover bcast and decision, not free when its IAR_PROPOSAL
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                RLO_msg_free(cur_wait_only_msg);
            }
            ret = 1;
        }
        cur_wait_only_msg = msg_t;
    }
    return ret;
}

RLO_msg_t* _find_proposal_msg(RLO_engine_t* eng, RLO_ID pid){
//    printf("%s:%d: searching pid = %d\n", __func__, __LINE__, pid);
    if(pid < 0)
        return NULL;

    RLO_msg_t* msg = eng->queue_iar_pending.head;
    //DEBUG_PRINT
    if(!msg)
        return NULL;
    //DEBUG_PRINT
    assert(msg->prop_state);
    //DEBUG_PRINT
    while(msg){
        if(msg->prop_state->pid == pid){
            return msg;
        }
        msg = msg->next;
    }
    //DEBUG_PRINT
    return NULL;
}

//Collecting
int _vote_merge(RLO_engine_t* eng, int pid, RLO_Vote vote_in, RLO_proposal_state* ps_out){
    RLO_msg_t* msg = _find_proposal_msg(eng, pid);
    if(!msg)
        return -1; //msg not found
    msg->prop_state->vote &= vote_in;
    msg->prop_state->votes_recved++;
    *ps_out = *(msg->prop_state);

    if(msg->prop_state->votes_needed == msg->prop_state->votes_recved)
        return 1;
    else
        return 0;

    return msg->prop_state->vote;
}

RLO_Req_stat _iar_check_status(RLO_engine_t* eng, int pid){
    RLO_msg_t* msg = _find_proposal_msg(eng, pid);
    if(msg)
        return msg->prop_state->state;
    return RLO_INVALID;
}

int _iar_pending_queue_update_status(RLO_engine_t* eng, int pid, RLO_Req_stat stat_in){
    RLO_msg_t* msg = _find_proposal_msg(eng, pid);
    if(!msg)
        return -1;
    msg->prop_state->state = stat_in;
    return 0;
}

// process queue for received proposals.
// Don't need to do anything???
int _iar_pending_queue_process(RLO_engine_t* eng, RLO_msg_t* decision_msg_in){//store received proposal msgs
    RLO_msg_t* msg = eng->queue_iar_pending.head;//all with in_progress status
    assert(msg->prop_state);

    while(msg){
        if(msg->prop_state->pid == decision_msg_in->prop_state->pid){
            //???
        }
        msg = msg->next;
    }
    return 0;
}

//msg is a recv_buf in bc_recv_buf_q, and already received data.
//Returns the cnt of sends.
int _bc_forward(RLO_engine_t* eng, RLO_msg_t* msg_in) {
    assert(msg_in);
    void *recv_buf;
    MPI_Status status = msg_in->irecv_stat;
    /* Increment # of messages received */
    eng->my_bcomm->bcast_recv_cnt++;

    /* Set buffer that message was received in */
    recv_buf = msg_in->msg_usr.buf;
    msg_in->send_cnt = 0;
    /* Check for a rank that can forward messages */
    int send_cnt = 0;;
    if (eng->my_bcomm->my_level > 0) {
        /* Retrieve message's origin rank */
        int origin = get_origin(recv_buf);
        send_cnt = 0;
        if (status.MPI_SOURCE > eng->my_bcomm->last_wall) {
            /* Send messages, to further ranks first */
            for (int j = eng->my_bcomm->send_channel_cnt; j >= 0; j--) {
                MPI_Isend(msg_in->msg_usr.buf, eng->my_bcomm->msg_size_max + sizeof(int), MPI_CHAR,
                        eng->my_bcomm->send_list[j], status.MPI_TAG, eng->my_bcomm->my_comm,
                        &(msg_in->bc_isend_reqs[j]));
                send_cnt++;
                msg_in->send_cnt++;
                //printf("%s:%u my rank = %03d, forward to rank %d, data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list[j], (char*)(msg_in->data_buf));
            }
            //printf("%s:%u my rank = %03d, append to queue_wait_and_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);

            if(status.MPI_TAG == RLO_BCAST){//bc
                queue_append(&(eng->queue_wait_and_pickup), msg_in);
                eng->fwd_queued++;
            }else{//iar_proposal, decision
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                if(status.MPI_TAG != RLO_IAR_DECISION)
                    queue_append(&(eng->queue_iar_pending), msg_in);
                eng->fwd_queued++;
            }
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
                        MPI_Isend(msg_in->msg_usr.buf, eng->my_bcomm->msg_size_max, MPI_CHAR, eng->my_bcomm->send_list[j],
                                status.MPI_TAG, eng->my_bcomm->my_comm,
                                &(msg_in->bc_isend_reqs[j]));
                        send_cnt++;
                        msg_in->send_cnt++;
                        //printf("%s:%u my rank = %03d, forward to rank %d, data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list[j], (char*)(msg_in->data_buf));
                    }
                }// end for
                //printf("%s:%u - rank = %03d, msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in);
                if(msg_in->send_cnt > 0){
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    //printf("%s:%u my rank = %03d, append to queue_wait_and_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
                    if(status.MPI_TAG == RLO_BCAST){
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        queue_append(&(eng->queue_wait_and_pickup), msg_in);
                        eng->fwd_queued++;
                    }else{//iar_proposal, decision
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        if(status.MPI_TAG != RLO_IAR_DECISION)
                            queue_append(&(eng->queue_iar_pending), msg_in);
                        eng->fwd_queued++;
                    }

                } else {
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    //printf("%s:%u my rank = %03d, append to queue_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
                    if(status.MPI_TAG == RLO_BCAST){
                        msg_in->fwd_done = 1;
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        queue_append(&(eng->queue_pickup), msg_in);
                        eng->fwd_queued++;
                    }else{//iar_proposal, decision
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        if(status.MPI_TAG != RLO_IAR_DECISION)
                            queue_append(&(eng->queue_iar_pending), msg_in);
                        eng->fwd_queued++;
                    }
                }
            } /* end if */
            else {
                //printf("%s:%u - rank = %03d Something is wrong... upper_bound = %d, add to pickup queue. msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, upper_bound, msg_in->data_buf);
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                if(status.MPI_TAG == RLO_BCAST){
                    msg_in->fwd_done = 1;
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    queue_append(&(eng->queue_pickup), msg_in);
                    eng->fwd_queued++;
                } else {//iar_proposal, decision
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    if(status.MPI_TAG != RLO_IAR_DECISION)
                        queue_append(&(eng->queue_iar_pending), msg_in);
                    eng->fwd_queued++;
                }
            }
        } /* end else */
    } /* end if -- */
    else {
        // Leaf rank, no forward, move to pickup_only.
        msg_in->fwd_done = 1;

        //printf("%s:%u my rank = %03d, append to queue_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        if(status.MPI_TAG == RLO_BCAST){
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            queue_append(&(eng->queue_pickup), msg_in);
            eng->fwd_queued++;
        }else{
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            if(status.MPI_TAG != RLO_IAR_DECISION)
                queue_append(&(eng->queue_iar_pending), msg_in);
        }
    }
    return send_cnt;
}

int _iar_process_infra_q_msg(RLO_msg_t* msg) {
    return -1;
}

/* ----------------- refactoring for progress engine END ----------------- */
/* ----------------------------------------------------------------------- */
/* ----------------------------------------------------------------------- */

int proposal_state_init(RLO_proposal_state* new_prop_state, RLO_msg_t* prop_msg_in) {
    if(!new_prop_state)
        return -1;
    new_prop_state->pid = -1;
    new_prop_state->pid = -1;
    new_prop_state->proposal_msg = prop_msg_in;
    new_prop_state->decision_msg = NULL;
    new_prop_state->recv_proposal_from = -1;
    new_prop_state->vote = 1;
    new_prop_state->votes_needed = -1;
    new_prop_state->votes_recved = 0;
    new_prop_state->state = RLO_INVALID;
    return 0;
}

//return index, or error.
int proposalPool_proposal_add(RLO_proposal_state* pools, RLO_proposal_state* pp_in) {//add new, or merge value
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
int proposalPool_proposal_setNeededVoteCnt(RLO_proposal_state* pools, RLO_ID k, int cnt) {
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

int proposalPool_vote_merge(RLO_proposal_state* pools, RLO_ID k, RLO_Vote v, int* p_index_out) {
    if(!pools)
        return -1;

    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {//exists, merge
        if(pools[i].pid == k) {
            pools[i].votes_recved++;
            pools[i].vote &= v;
            *p_index_out = i;
            printf("%s:%u - recived votes cnt = %d, needed = %d\n", __func__, __LINE__, pools[i].votes_recved, pools[i].votes_needed);
            if(pools[i].votes_recved == pools[i].votes_needed){//votes done
                return 1;
            } else
                return 0;
        }
    }
    // id not found, need to add new one, not just merge
    return -2;
}

int proposalPool_get(RLO_proposal_state* pools, RLO_ID k, RLO_proposal_state* result) {
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

int proposalPool_get_index(RLO_proposal_state* pools, RLO_ID k) {
    if(!pools)//null
        return -1;
    int i = 0;
    for(i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        if(pools[i].pid == k)
            return i;
    }
    return -1;//not found
}

int proposalPool_rm(RLO_proposal_state* pools, RLO_ID k) {
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

int proposalPools_reset(RLO_proposal_state* pools) {
    for (int i = 0; i <= PROPOSAL_POOL_SIZE - 1; i++) {
        pools[i].pid = -1;
        pools[i].vote = 1;
        pools[i].recv_proposal_from = -1;
        pools[i].votes_needed = -1;
        pools[i].votes_recved = -1;
        pools[i].proposal_msg = NULL;
        pools[i].state = RLO_INVALID;
    }
    return 0;
}

//TODO: using offsetof(sth) and pointers with complex MPI data types avoid memcpy from user buf;

int pbuf_vote_serialize(int my_rank, RLO_ID pid_in, RLO_Vote vote, void** buf_out, size_t* buf_len_out){
    size_t total = sizeof(int) + sizeof(RLO_ID) + sizeof(RLO_Vote)+ sizeof(RLO_time_stamp)+ sizeof(size_t) + 0;

    size_t data_len = 0;
    RLO_time_stamp ts = 0;
//    printf("%s:%d: pid = %d, vote = %d, time = %lu, data_len = %lu, total_len = %lu\n",
//            __func__, __LINE__, pid_in, vote, ts, data_len, total);
    if(!(*buf_out))
        *buf_out = calloc(1, total + 1);

    void* cur = *buf_out;

    *(int*)cur = my_rank;
    cur = (char*)cur + sizeof(int);

    *(RLO_ID*)cur = pid_in;
    cur = (char*)cur + sizeof(RLO_ID);

    *(RLO_Vote*)cur = vote;
    cur = (char*)cur + sizeof(RLO_Vote);

    *(RLO_time_stamp*)cur = ts;
    cur = (char*)cur + sizeof(RLO_time_stamp);


    *(size_t*)cur = data_len;
    cur = (char*)cur + sizeof(size_t);

    cur = NULL;

    *buf_len_out = total;
    return 0;
}
int pbuf_serialize(RLO_ID pid_in, RLO_Vote vote, RLO_time_stamp time_stamp, size_t data_len_in, void* data_in,
        void** buf_out, size_t* buf_len_out) {
    size_t total = sizeof(RLO_ID) + sizeof(RLO_Vote)+ sizeof(RLO_time_stamp)+ sizeof(size_t) + data_len_in;
    printf("%s:%d: pid = %d, vote = %d, time = %lu, data_len = %lu, total_len = %lu\n",
            __func__, __LINE__, pid_in, vote, time_stamp, data_len_in, total);
    if(!(*buf_out))
        *buf_out = calloc(1, total);

    if(data_len_in == 0) {
        if(data_in != NULL)
            return -1;
    }

    if(!buf_out)
        return -1;

    void* cur = *buf_out;
    *(RLO_ID*)cur = pid_in;
    cur = (char*)cur + sizeof(RLO_ID);

    *(RLO_Vote*)cur = vote;
    cur = (char*)cur + sizeof(RLO_Vote);

    *(RLO_time_stamp*)cur = time_stamp;
    cur = (char*)cur + sizeof(RLO_time_stamp);


    *(size_t*)cur = data_len_in;
    cur = (char*)cur + sizeof(size_t);

    if(data_len_in == 0)
        cur = NULL;
    else
        memcpy(cur, data_in, data_len_in);

    *buf_len_out = total;
//    return total;
//
//    *((RLO_ID*)buf_out) = pid_in;
//    *((RLO_Vote*)(buf_out + sizeof(RLO_ID))) = vote;
//    *((RLO_time_stamp*)(buf_out + sizeof(RLO_ID) + sizeof(RLO_Vote))) = time_stamp;
//    *((size_t*)(buf_out + sizeof(RLO_ID) + sizeof(RLO_Vote)) + sizeof(RLO_time_stamp)) = data_len_in;
//
//    if(data_len_in != 0){
//        memcpy(buf_out + sizeof(RLO_ID) + sizeof(RLO_Vote) + sizeof(RLO_time_stamp) + sizeof(size_t), data_in, data_len_in);
//    }
//    if(buf_len_out) {
//        *buf_len_out = sizeof(RLO_ID)  /* SN */
//            + sizeof(RLO_Vote)          /* vote/decision */
//            + sizeof(RLO_time_stamp)    /* Time stamp */
//            + sizeof(size_t)  /* data_len */
//            + data_len_in;          /* data */
//    }

    return 0;
}

PBuf* pbuf_new_local(RLO_ID pid_in, RLO_Vote vote, RLO_time_stamp time_stamp, size_t data_len_in, void* data_in){
    PBuf* pbuf = calloc(1, sizeof(PBuf));
    pbuf->pid = pid_in;
    pbuf->vote = vote;
    pbuf->time_stamp = time_stamp;
    pbuf->data_len = data_len_in;
    pbuf->data = data_in;
    return pbuf;
}

size_t pbuf_serialize_pb(PBuf* pb_in, void** buf_out){
    assert(pb_in);
    size_t buf_size = 0;
    pbuf_serialize(pb_in->pid, pb_in->vote, pb_in->time_stamp, pb_in->data_len, pb_in->data, buf_out, &buf_size);
    return buf_size;
}
void pbuf_free(PBuf* pbuf) {
    assert(pbuf);
    if(!pbuf->data)
        free(pbuf->data);
    free(pbuf);
}

int pbuf_deserialize(void* buf_in, PBuf** pbuf_out) {
    assert(buf_in);
    if(!*pbuf_out)
        *pbuf_out = calloc(1, sizeof(PBuf));

    (*pbuf_out)->pid = *(RLO_ID*)buf_in;
    buf_in = (char*)buf_in + sizeof(RLO_ID);

    (*pbuf_out)->vote = *(RLO_Vote*)buf_in;
    buf_in = (char*)buf_in + sizeof(RLO_Vote);

    (*pbuf_out)->time_stamp = *(RLO_time_stamp*)buf_in;
    buf_in = (char*)buf_in + sizeof(RLO_time_stamp);

    (*pbuf_out)->data_len = *(size_t*)buf_in;
    int data_len = *(size_t*)buf_in;
    buf_in = (char*)buf_in + sizeof(size_t);

//    printf("[Process %d] %s:%d: pid = %d, vote = %d, time = %lu, data_len = %lu, \n",
//            getpid(), __func__, __LINE__, (*pbuf_out)->pid, (*pbuf_out)->vote, (*pbuf_out)->time_stamp, (*pbuf_out)->data_len);

    if((*pbuf_out)->data_len == 0)
        (*pbuf_out)->data = NULL;
    else
        (*pbuf_out)->data = calloc(1, (*pbuf_out)->data_len);
    memcpy((*pbuf_out)->data, buf_in, (*pbuf_out)->data_len);

    return data_len;
}

void pbuf_debug(PBuf* p, void* serialized_buf_in){
    int ret = 0;
    PBuf* t = NULL;
    pbuf_deserialize(serialized_buf_in, &t);

    assert(p->pid == t->pid);
    assert(p->time_stamp == t->time_stamp);
    assert(p->vote == t->vote);
    assert(p->data_len = t->data_len);
    if(p->data_len > 0){
        assert(memcmp(p->data, t->data, p->data_len) == 0);
    }
    printf("%s:%d pbuf_debug test succeeded.\n", __func__, __LINE__);
}

RLO_ID make_pid(bcomm* my_bcomm) {
    return (RLO_ID) my_bcomm->my_rank;
}

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
    if (rank == 0) {
        if (is_powerof2(world_size))
            return log2(world_size) - 1;
        else
            return log2(world_size);
    }

    int l = 0;
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
    //my_bcomm->my_comm = comm;
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    if (my_bcomm->world_size < 2) {
        printf("Too few ranks, program ended. world_size = %d\n", my_bcomm->world_size);
        return NULL;
    }
    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);
    int my_rank = my_bcomm->my_rank;
    /* Set operation counters */
    my_bcomm->my_bcast_cnt = 0;
    my_bcomm->bcast_recv_cnt = 0;

    /* Message fields */
    my_bcomm->msg_size_max = RLO_MSG_SIZE_MAX;
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
        for (int i = 0; i < my_bcomm->send_list_len; i++){
            my_bcomm->send_list[i] = (int) (my_bcomm->my_rank + pow(2, i)) % my_bcomm->world_size;
        }
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
    my_bcomm->bcast_send_cnt = 0;

//    printf("%s:%u - rank = %03d, level = %d, send_channel_cnt = %d, send_list_len = %d\n",
//            __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_channel_cnt,
//            my_bcomm->send_list_len);
    return my_bcomm;
}

void bcomm_free(bcomm * my_bcomm){
    free(my_bcomm->send_list);
    free(my_bcomm);
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

//return the number of sends if forward
int fwd_send_cnt(const bcomm* my_bcomm, int origin_rank, int from_rank) {
    int send_cnt = 0;
    int upper_bound = my_bcomm->send_channel_cnt - 1;

    if (my_bcomm->my_level > 0) {
        if (from_rank > my_bcomm->last_wall) {
            for (int j = my_bcomm->send_channel_cnt; j >= 0; j--) {
                send_cnt++;
            }
        } else {
            if (upper_bound >= 0) {
                for (int i = upper_bound; i >= 0; i--) {
                    if (check_passed_origin(my_bcomm, origin_rank, my_bcomm->send_list[i]) == 0) {
                        send_cnt++;
                    }
                }
            } // else: 0
        }
    }
    return send_cnt;
}

int RLO_bcast_gen(RLO_engine_t* eng, RLO_msg_t* msg_in, enum RLO_COMM_TAGS tag) {
    bcomm* my_bcomm = eng->my_bcomm;
    msg_in->bc_init = 1; // just to ensure.
    msg_in->pickup_done = 1; // bc msg doesn't need pickup.

    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--) {
        MPI_Isend(msg_in->msg_usr.buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm,
            &(msg_in->bc_isend_reqs[i]));
        msg_in->send_cnt++;
    }

    msg_in->send_type = tag;
    queue_append(&(eng->queue_wait), msg_in);

    if(tag != RLO_IAR_PROPOSAL){
        eng->sent_bcast_cnt++;
    }
    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    RLO_make_progress();
    return 0;
}

int RLO_progress_engine_cleanup(RLO_engine_t* eng){
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat_out1;
    int done = 0;
    int my_rank = RLO_get_my_rank();
    RLO_msg_t* recv_msg;
    MPI_Iallreduce(&(eng->sent_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, eng->my_bcomm->my_comm, &req);

    do {
        MPI_Test(&req, &done, &stat_out1);// test for MPI_Iallreduce.
        if (!done) {
            RLO_make_progress();
        }
    } while (!done);

    // Core cleanup section
    while (eng->recved_bcast_cnt + eng->sent_bcast_cnt < total_bcast) {
        RLO_make_progress();
    }
    recv_msg = NULL;
    RLO_user_msg* pickup_out = NULL;
    while(RLO_user_pickup_next(eng, &pickup_out)){
        total_pickup++;
        //printf("%s:%u - rank = %03d, pickup_out msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, pickup_out->data_buf);
    }
    RLO_msg_t* tmp = eng->queue_recv.head;

    while(tmp){
        RLO_msg_t* t = tmp->next;
        MPI_Cancel(&(tmp->irecv_req));
        RLO_msg_free(tmp);
        tmp = t;
    }
    bcomm_free(eng->my_bcomm);

    printf("%s:%u, pid = %d, engine_cnt = %d, engine_id = %d\n", __func__, __LINE__, getpid(), Active_Engines->engine_cnt, eng->engine_id);
    engine_remove(Active_Engines, eng);
    printf("%s:%u, pid = %d, engine_cnt = %d, engine_id = %d\n", __func__, __LINE__, getpid(), Active_Engines->engine_cnt, eng->engine_id);
    progress_engine_free(eng);
    return 0;
}

int RLO_proposal_reset(RLO_proposal_state* ps){
    assert(ps);
    if(ps->decision_msg)
        RLO_msg_free(ps->decision_msg);
    ps->decision_msg = NULL;
    if(ps->proposal_msg)
        RLO_msg_free(ps->proposal_msg);
    ps->proposal_msg = NULL;
    ps->pid = -1;
    ps->recv_proposal_from = -1;
    ps->state = RLO_INVALID;
    ps->vote = -1;
    ps->votes_needed = 0;
    ps->votes_recved = 0;
    return 0;
}

int RLO_get_vote_my_proposal(RLO_engine_t* eng){
    if(eng->my_own_proposal.state != RLO_COMPLETED){
        return -1;
    }
    int ret = eng->my_own_proposal.vote;
    //RLO_proposal_reset(&(eng->my_own_proposal));
    return ret;
}

int native_benchmark_single_point_bcast(MPI_Comm my_comm, int root_rank, int cnt, int buf_size) {
    char* buf = calloc(buf_size, sizeof(char));
    char recv_buf[RLO_MSG_SIZE_MAX] = {'\0'};
    // native mpi bcast

    int my_rank;
    MPI_Comm_rank(my_comm, &my_rank);

    if(my_rank == root_rank) {
        //sleep(1);
        unsigned long start = RLO_get_time_usec();
        MPI_Barrier(my_comm);
        for(int i = 0; i < cnt; i++) {
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
            MPI_Bcast(buf, buf_size, MPI_CHAR, root_rank, my_comm);
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        }
        unsigned long end = RLO_get_time_usec();
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

int queue_test(int cnt){
    queue q;
    q.head = NULL;
    q.tail = NULL;
    q.msg_cnt = 0;

    for(int i = 0; i < cnt; i++){
        RLO_msg_t* new_msg = calloc(1, sizeof(RLO_msg_t));
        new_msg->id_debug = i;
        new_msg->fwd_done = 1;
        new_msg->pickup_done = 1;
        queue_append(&q, new_msg);
    }

    RLO_msg_t* cur = q.head;
    printf("cur = %p\n", cur);
    while(cur){
        printf("Looping queue after appending: msg->id_debug = %d\n", cur->id_debug);
        cur = cur->next;
    }

    cur = q.head;
    printf("cur = %p, q.cnt = %d\n", cur, q.msg_cnt);
    while(cur){
        printf("Remove element: msg->id_debug = %d\n", cur->id_debug);
        RLO_msg_t* t = cur->next;
        queue_remove(&q, cur);
        free(cur);
        cur = t;
    }
    cur = q.head;
    printf("After removing, q.head = %p, q.cnt = %d\n", cur, q.msg_cnt);

    while(cur){
        printf("Looping queue after removing: msg->id_debug = %d\n", cur->id_debug);
        cur = cur->next;
    }
    return 0;
}
