/*
 * rootless_ops.c
 *
 *  Created on: Aug, 2018
 *      Author: Tonglin Li
 */

#include "rootless_ops.h"
//#include "progress_engine.h"



#define PROPOSAL_POOL_SIZE 16 //maximal concurrent proposal supported
#define ISEND_CONCURRENT_MAX 128 //maximal number of concurrent and unfinished isend, used to set MPI_Request and MPI_State arrays for MPI_Waitall().

enum MSG_TAGS {//Used as a msg tag, it's a field of the msg. Class 2
    IAR_Vote // to replace IAR_VOTE in mpi_tag
};

typedef struct queue {
    bcomm_GEN_msg_t* head;
    bcomm_GEN_msg_t* tail;
    int msg_cnt;
}queue;

int queue_append(queue* q, bcomm_GEN_msg_t* msg);
int queue_remove(queue* q, bcomm_GEN_msg_t* msg);


typedef struct bcomm_token_t {
    ID req_id;
    Req_stat req_stat;
    bool completed;
    bool vote_result;
    struct bcomm_token_t* next;
} bcomm_token_t;

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

char DEBUG_MODE = 'O';
typedef struct {
    FILE* log_file;
    int my_rank;
} Log;
Log MY_LOG;

// Skip ring utility funcitons
int get_origin(void* buf_in);
int check_passed_origin(const bcomm* my_bcomm, int origin_rank, int to_rank);
int fwd_send_cnt(const bcomm* my_bcomm, int origin_rank, int from_rank);//return # of sends needed if forward a msg

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

int get_my_rank(){
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    return my_rank;
}

int get_level(int world_size, int rank);

int proposal_state_init(proposal_state* pp_in_out, bcomm_GEN_msg_t* prop_msg_in);
//Proposal pool ops

int proposalPool_proposal_add(proposal_state* pools, proposal_state* pp_in);
int proposalPool_proposal_setNeededVoteCnt(proposal_state* pools, ID k, int cnt);
int proposalPool_vote_merge(proposal_state* pools, ID k, Vote v, int* p_index_out);
int proposalPool_get(proposal_state* pools, ID k, proposal_state* result);
int proposalPool_get_index(proposal_state* pools, ID k);
int proposalPool_rm(proposal_state* pools, ID k);
int proposalPools_reset(proposal_state* pools);

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
    ID pid; // proposal ID, default = -1;
    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
//    int proposal_sent_cnt;
    Vote vote; //accumulated vote, default = 1;
    int votes_needed; //num of votes needed to report to parent, equals to the number of sends of a proposal
    int votes_recved;
    Req_stat state; //the state of this proposal: COMPLETED, IN_PROGRESS or FAILED.
    bcomm_GEN_msg_t* proposal_msg;//the last place holds a proposal, should be freed when decision is made and executed.
    bcomm_GEN_msg_t* decision_msg;
}; //clear when vote result is reported.

struct bcomm_IAR_state_t {
    proposal_state prop_state;
    //enum State_IAR iar_state;
    bcomm_IAR_state_t *next, *prev;
};

struct bcomm_progress_engine {
    bcomm *my_bcomm;
    //generic queues for bc
    queue queue_recv;
    queue queue_wait;//waiting for isend completion.
    queue queue_pickup; //ready for pickup
    queue queue_wait_and_pickup;//act like have both two roles above

    queue queue_iar_pending; //store received proposal msgs

    bcomm_GEN_msg_t
        *rcv_q_head,
        *rcv_q_tail; // Used by BC, proposal, vote, decision

    unsigned int bc_incomplete;
    unsigned int recved_bcast_cnt;
    unsigned int sent_bcast_cnt;

    //=================   IAR   =================
    void* user_iar_ctx;
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

    iar_cb_func_t prop_judgement_cb; //provided by the user, used to judge if agree with a proposal
    void *app_ctx;
    iar_cb_func_t proposal_action; //if a proposal is approved, what to do with it.

    //debug variables
    int fwd_queued;
};

int msg_test_isends(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in);
int msg_wait(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in);

int _test_completed(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf);

int _gen_bc_msg_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);

//Generic function, to (re)post a irecv. Used by BC, IAR and all other places that need a buff to recv.
int _post_irecv_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf, enum COM_TAGS rcv_tag);

//Progress engine queue process functions
int _wait_and_pickup_queue_process(bcomm_engine_t* en, bcomm_GEN_msg_t* msg);
int _wait_only_queue_cleanup(bcomm_engine_t* eng);

// actions for proposals, votes and decisions. Called in make_progress_gen() loop.
int _iar_proposal_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);
int _iar_vote_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);
int _iar_decision_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in);

int _vote_back(bcomm_engine_t* eng, proposal_state* ps, Vote vote);
int _iar_decision_bcast(bcomm_engine_t* eng, ID my_proposal_id, Vote decision);
int _vote_merge(bcomm_engine_t* eng, int pid, Vote vote_in, proposal_state* ps_out);

bcomm_GEN_msg_t* _find_proposal_msg(bcomm_engine_t* eng, ID pid);

//Type for user callback functions
typedef struct proposals_ctx{

}proposals_ctx;

int _bc_forward(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg);//new version

//For irecv and other generic use
bcomm_GEN_msg_t* msg_new_generic(bcomm_engine_t* eng) {
    bcomm_GEN_msg_t* new_msg = calloc(1, sizeof(bcomm_GEN_msg_t));
    //printf("%s:%u - rank = %03d: new_msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, new_msg);
    new_msg->msg_usr.pid = -1;
    new_msg->msg_usr.type = -1;
    new_msg->msg_usr.vote = -1;
    new_msg->data_buf = new_msg->msg_usr.buf + sizeof(int);
    new_msg->bc_isend_reqs = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Request));
    new_msg->bc_isend_stats = calloc(eng->my_bcomm->send_list_len, sizeof(MPI_Status));
    new_msg->pickup_done = 0;
    new_msg->bc_init = 0;//by default 0, when created to be a recv buf.
    new_msg->prev = NULL;
    new_msg->next = NULL;
    new_msg->send_cnt = 0;

    new_msg->msg_usr.data = new_msg->msg_usr.buf + sizeof(int);// == msg->data_buf.
    // Set msg origin
    memcpy(new_msg->msg_usr.buf, &(eng->my_bcomm->my_rank), sizeof(int));
    *(int*)(new_msg->msg_usr.buf) = eng->my_bcomm->my_rank;
    return new_msg;
}

bcomm_GEN_msg_t* msg_new_bc(bcomm_engine_t* eng, void* buf_in, int send_size) {
    bcomm_GEN_msg_t* new_msg = msg_new_generic(eng);

    memcpy(new_msg->data_buf, buf_in, send_size);
    new_msg->bc_init = 1;//by default. set to 0 when created to be a recv buf.
    return new_msg;
}

int msg_test_isends(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
    assert(eng);
    assert(msg_in);
    int completed = 0;
    MPI_Testall(msg_in->send_cnt, msg_in->bc_isend_reqs, &completed, msg_in->bc_isend_stats);
    return completed;
}

int msg_wait(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
    return MPI_Waitall(eng->my_bcomm->send_list_len, msg_in->bc_isend_reqs, msg_in->bc_isend_stats);
}

int msg_free(bcomm_GEN_msg_t* msg_in) {
    int my_rank = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    //printf("Rank %d: msg_free: msg_in = %p\n", my_rank, msg_in);
    //printf("Rank %d: msg_free: bc_isend_reqs = %p\n", my_rank, msg_in->bc_isend_reqs);
    free(msg_in->bc_isend_reqs);
    //printf("Rank %d: msg_free: bc_isend_stats = %p\n", my_rank, msg_in->bc_isend_stats);
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
int queue_remove(queue* q, bcomm_GEN_msg_t* msg){
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

bcomm_engine_t* progress_engine_new(bcomm* my_bcomm, void* approv_cb_func, void* app_ctx, void* app_proposal_action){

    bcomm_engine_t* eng = calloc(1, sizeof(bcomm_engine_t));

    eng->my_bcomm = my_bcomm;

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

    proposal_state new_prop_state;
    proposal_state_init(&new_prop_state, NULL);

    eng->my_own_proposal = new_prop_state;

    bcomm_GEN_msg_t* msg_irecv_init = msg_new_generic(eng);
    eng->rcv_q_head = msg_irecv_init;
    eng->rcv_q_tail = msg_irecv_init;

    eng->fwd_queued = 0;

    proposalPools_reset(eng->proposal_state_pool);

    _post_irecv_gen(eng, msg_irecv_init, ANY_TAG);
    queue_append(&(eng->queue_recv), msg_irecv_init);
    return eng;
}

//Turn the gear. Output a handle(recv_msgs_out ) to the received msg, for sampling purpose only. User should use pickup_next() to get msg.
int make_progress_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t** recv_msg_out) {

    //========================== My active proposal state update==========================
    if(eng->my_own_proposal.state != COMPLETED && eng->my_own_proposal.state != INVALID){// if I have an active proposal
        //printf("%s:%u - rank = %03d: pid = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.pid);
        if(eng->my_own_proposal.decision_msg){
            //printf("%s:%u - rank = %03d: decision_msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.decision_msg);
            if(msg_test_isends(eng, eng->my_own_proposal.decision_msg)){
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                eng->my_own_proposal.state = COMPLETED;
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                msg_free(eng->my_own_proposal.decision_msg);
                eng->my_own_proposal.decision_msg = NULL; //freed after pickup.
            }
        }
    }

    //========================== Bcast msg handling ==========================
    bcomm_GEN_msg_t* cur_bc_rcv_buf = eng->queue_recv.head;//eng->rcv_q_head;
    while(cur_bc_rcv_buf) {//receive and repost with tag = BCAST

        bcomm_GEN_msg_t* msg_t = cur_bc_rcv_buf->next;

        if(_test_completed(eng, cur_bc_rcv_buf)){//irecv complete.
            int recv_tag = cur_bc_rcv_buf->irecv_stat.MPI_TAG;
            //printf("%s:%u - rank = %03d, recv_tag = %d, src = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_tag, cur_bc_rcv_buf->irecv_stat.MPI_SOURCE);
            queue_remove(&(eng->queue_recv), cur_bc_rcv_buf);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            bcomm_GEN_msg_t* msg_new_recv = msg_new_generic(eng);
            _post_irecv_gen(eng, msg_new_recv, ANY_TAG);//cur_bc_rcv_buf->irecv_stat.MPI_TAG
            //printf("%s:%u - rank = %03d, msg_new_recv = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_new_recv);
            queue_append(&(eng->queue_recv), msg_new_recv);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

            switch(recv_tag){
                case BCAST: {
                    eng->recved_bcast_cnt++;
                    _bc_forward(eng, cur_bc_rcv_buf);
                    if(recv_msg_out)
                        *recv_msg_out = cur_bc_rcv_buf;
                    break;
                }

                case IAR_PROPOSAL: {
                    //processed by a callback function, not visible to the users
                    //do not increase eng->recved_bcast_cnt
                    _iar_proposal_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                case IAR_VOTE: {
                    _iar_vote_handler(eng, cur_bc_rcv_buf);
                    break;
                }

                case IAR_DECISION: {
                    eng->recved_bcast_cnt++;

                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    // remove corresponding proposal state
                    _iar_decision_handler(eng, cur_bc_rcv_buf);

                    //msg logic is same as BCAST, and will always end up being picked up.
                    _bc_forward(eng, cur_bc_rcv_buf);//queue ops happen here

                    if(recv_msg_out)
                        *recv_msg_out = cur_bc_rcv_buf;
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    break;
                }

                default: {
                    printf("%s:%u - rank = %03d: received a msg with unknown tag: %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, recv_tag);
                    break;
                }
            }
        }
        cur_bc_rcv_buf = msg_t;//move cursor
    }//loop through bc recv queue

    //============================ BC Wait queue processing =======================
    bcomm_GEN_msg_t* cur_wait_pickup_msg = eng->queue_wait_and_pickup.head;
    while(cur_wait_pickup_msg){
        bcomm_GEN_msg_t* msg_t = cur_wait_pickup_msg->next;
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        _wait_and_pickup_queue_process(eng, cur_wait_pickup_msg);
        cur_wait_pickup_msg = msg_t;
    }

    //================= Cleanup wait_only and pickup_only queues ==================
    //clean up all done msgs in the wait_only queue.
    _wait_only_queue_cleanup(eng);

    //clean up pickup_only queue is done by user_pickup_next().

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
    int ret = MPI_Irecv(msg_buf_in_out->msg_usr.buf, eng->my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, MPI_ANY_SOURCE, rcv_tag, eng->my_bcomm->my_comm, &(msg_buf_in_out->irecv_req));
    //msg_buf_in_out->id_debug = 1;
//    printf("%s:%u - rank = %03d, irecv posted on msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf_in_out);
    return ret;
}

int _proposal_pickup_next(){
    return -1;
}



typedef struct application_context{

}App_ctx;
//decide A proposal is approved, no matter it's mine or others'?
// Return 1, if mine is declined, others' win;
// return 0 if mine wins, others' declined
// return 2 if both wins: compatible




int _iar_proposal_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in) {
    if (!eng || !recv_msg_buf_in)
        return -1;

    PBuf* pbuf = malloc(sizeof(PBuf));

    int origin = get_origin(recv_msg_buf_in->msg_usr.buf);

    pbuf_deserialize(recv_msg_buf_in->data_buf, pbuf);

    //add a state to waiting_votes queue.
    proposal_state* new_prop_state = malloc(sizeof(proposal_state));
    //need to read pid from msg and fill in state.
    proposal_state_init(new_prop_state, recv_msg_buf_in);

    new_prop_state->proposal_msg = recv_msg_buf_in;
    new_prop_state->pid = pbuf->pid;
    new_prop_state->recv_proposal_from = recv_msg_buf_in->irecv_stat.MPI_SOURCE;
    new_prop_state->state = IN_PROGRESS;
    recv_msg_buf_in->prop_state = new_prop_state;
    printf("%s:%u - rank = %03d: received a proposal from rank %d: %p\n",
            __func__, __LINE__, eng->my_bcomm->my_rank, recv_msg_buf_in->irecv_stat.MPI_SOURCE, recv_msg_buf_in);
    if(pbuf->pid == eng->my_own_proposal.pid){
        printf("%s:%u - rank = %03d: received a proposal with my own pid: something went wrong...\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        return -1;
    }else{
        new_prop_state->votes_needed = fwd_send_cnt(eng->my_bcomm, origin, recv_msg_buf_in->irecv_stat.MPI_SOURCE);//votes needed;
        printf("%s:%u - rank = %03d: need %d votes. \n", __func__, __LINE__, eng->my_bcomm->my_rank, new_prop_state->votes_needed);
    }

//    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    //eng->app_ctx = eng->my_proposal;
    int judgment = (eng->prop_judgement_cb)(pbuf->data, eng->app_ctx);//received proposal and my proposal
    printf("%s:%u - rank = %03d, prop_judgement_cb() = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, judgment);
    switch (judgment) {
        case 0: {    //others' declined
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            //queue_append(&(eng->queue_iar_pending), recv_msg_buf_in);
            _vote_back(eng, new_prop_state, 0);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            break;
        }
        case 1: {    // others' win. is mine declined?
                //Add msg to queue_iar_pending in _bc_forward().

            int fwd_cnt = fwd_send_cnt(eng->my_bcomm, origin, recv_msg_buf_in->irecv_stat.MPI_SOURCE);
            printf("%s:%u - rank = %03d: eng->queue_iar_pending.head = %p, queueing a proposal: %p, fwd_cnt = %d\n",
                    __func__, __LINE__, eng->my_bcomm->my_rank, eng->queue_iar_pending.head, recv_msg_buf_in, fwd_cnt);

            if (_bc_forward(eng, recv_msg_buf_in) == 0) {//no need to fwd.
                _vote_back(eng, new_prop_state, 1);
            }
            break;
        }
        default:
            printf("%s:%u - rank = %03d: unknown judgment received: %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, judgment);
            break;
    }
    pbuf_free(pbuf);
    return 0;
}

int _vote_back(bcomm_engine_t* eng, proposal_state* ps, Vote vote){
    printf("%s:%u - rank = %03d, vote back to rank %d, for pid = %d, vote = %d.\n", __func__, __LINE__,eng->my_bcomm->my_rank, ps->recv_proposal_from, ps->pid, vote);
    size_t send_len = 0;
    char send_buf[MSG_SIZE_MAX+1] = "";

    memcpy(send_buf, &(eng->my_bcomm->my_rank), sizeof(int));
    pbuf_serialize(ps->pid, vote, 0, NULL, send_buf + sizeof(int), &send_len);
    MPI_Send(send_buf, eng->my_bcomm->msg_size_max + 1, MPI_CHAR, ps->recv_proposal_from,
            IAR_VOTE, eng->my_bcomm->my_comm);
//    MPI_Request req;
//    MPI_Isend(send_buf, eng->my_bcomm->msg_size_max + 1, MPI_CHAR, ps->recv_proposal_from,
//            IAR_VOTE, eng->my_bcomm->my_comm, &req);
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    return send_len;
}

int _iar_vote_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf) {
    if (!eng || !msg_buf)
        return -1;

    //update proposal_state_queue
    //decide if all necessary votes are received, then vote back
    PBuf* vote_buf = malloc(sizeof(PBuf));

    pbuf_deserialize(msg_buf->data_buf, vote_buf);        //votes have same format as all other msgs

//    printf("%s:%u - rank = %03d: received a vote = %d for pid = %d\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, vote_buf->vote, vote_buf->pid);

    if (vote_buf->pid == eng->my_own_proposal.pid) { //votes for my proposal
//        printf("%s:%u - rank = %03d, received a vote from rank %03d for my proposal, vote = %d.\n", __func__, __LINE__,
//                eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE, vote_buf->vote);
        eng->my_own_proposal.votes_recved++;
        eng->my_own_proposal.vote &= vote_buf->vote; //*(Vote*)(vote_buf->data);
        printf("%s:%u - rank = %03d, "
                "received a vote from rank %03d for my proposal, vote = %d, "
                "received %d votes, needed %d votes.\n", __func__, __LINE__,
                eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE,
                vote_buf->vote, eng->my_own_proposal.votes_recved,
                eng->my_own_proposal.votes_needed);
        if (eng->my_own_proposal.votes_recved == eng->my_own_proposal.votes_needed) { //all done, bcast decision.
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

            if(eng->my_own_proposal.vote){
                printf("%s:%u - rank = %03d, app_ctx = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->app_ctx);

                eng->my_own_proposal.vote = (eng->prop_judgement_cb)(eng->my_proposal, eng->app_ctx);
                printf("%s:%u - rank = %03d, prop_judgement_cb() = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_own_proposal.vote);
            }
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            _iar_decision_bcast(eng, eng->my_own_proposal.pid, eng->my_own_proposal.vote);
            pbuf_free(vote_buf);
            return 0;
        } else { // need more votes for my decision, continue to irecv.
            pbuf_free(vote_buf);
            return 0;
        }

    } else { //Votes for proposals in the state queue
        printf("%s:%u - rank = %03d, received a vote from rank %03d for other's proposal, vote = %d.\n",
                __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf->irecv_stat.MPI_SOURCE, vote_buf->vote);
        proposal_state ps_result;
        int ret = _vote_merge(eng, vote_buf->pid, vote_buf->vote, &ps_result);
        //printf("%s:%u - rank = %03d, merged vote = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, ps_result.vote);
        //int ret = proposalPool_vote_merge(eng->proposal_state_pool, vote_buf->pid, vote_buf->vote, &p_index);

        if (ret < 0) {
            printf("Function %s:%u - rank %03d: can't merge vote, proposal not exists, pid = %d \n", __func__, __LINE__,
                    eng->my_bcomm->my_rank, vote_buf->pid);
            pbuf_free(vote_buf);
            return -1;
        } else { // Find proposal, merge completed.
            if (ret == 1) { //done collecting votes, vote back
                printf("%s:%u - rank = %03d: done collecting votes, vote back = %d for pid = %d, vote_buf pid = %d\n",
                        __func__, __LINE__, eng->my_bcomm->my_rank, ps_result.vote, ps_result.pid, vote_buf->pid);
                _vote_back(eng, &ps_result, ps_result.vote);
                pbuf_free(vote_buf);
            } else {
                printf("%s:%u - rank = %03d: merging done, waiting more votes for pid = %d, "
                        "current vote = %d. received %d votes, needed %d.\n", __func__, __LINE__,
                        eng->my_bcomm->my_rank, ps_result.pid, ps_result.vote, ps_result.votes_recved, ps_result.votes_needed);
            }
        }
    }
    return 0;
}

int _iar_decision_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_buf_in) {
    if(!eng || !msg_buf_in)
        return -1;

    //update proposal_state_queue
    PBuf* decision_buf = malloc(sizeof(PBuf));
    pbuf_deserialize(msg_buf_in->data_buf, decision_buf);
    //printf("%s: %d: rank = %03d, received a decision: %p = [%d:%d], prop_state = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_buf_in, decision_buf->pid, decision_buf->vote, msg_buf_in->prop_state);
    //int index = proposalPool_get_index(eng->proposal_state_pool, decision_buf->pid);

    bcomm_GEN_msg_t* proposal_msg = _find_proposal_msg(eng, decision_buf->pid);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    //I don't have this proposal, but received a decision about it. (could only be 0)
    // Don't need to forward, but need pickup.
    if(!proposal_msg){
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        queue_append(&(eng->queue_pickup), msg_buf_in);
        return -1;
    }

    if(decision_buf->vote == 0){//proposal canceled
        //printf("%s:%u - rank = %03d: received decision: proposal canceled: pid = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_buf->pid);
        queue_remove(&(eng->queue_iar_pending), proposal_msg);
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        msg_free(proposal_msg);
        //some cleanup ???
    } else {
        //execute proposal: a callback function
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        (eng->proposal_action)(proposal_msg->data_buf, eng->app_ctx);

        proposal_msg->prop_state->state = COMPLETED;
        queue_remove(&(eng->queue_iar_pending), proposal_msg);
        ////printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        msg_free(proposal_msg);
        //done using proposal msg.
        //printf("%s:%u - rank = %03d: proposal approved: pid = %d \n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_buf->pid);
    }

    //give user a notification
    msg_buf_in->fwd_done = 1;//set for so pickup queue can free it eventually.
    queue_append(&(eng->queue_pickup), msg_buf_in);

    pbuf_free(decision_buf);

    return 0;
}

int proposal_succeeded(bcomm_engine_t* eng){
    make_progress_gen(eng, NULL);

    return (eng->my_own_proposal.votes_needed == eng->my_own_proposal.votes_recved
            && eng->my_own_proposal.vote != 0);
    //succeed, fail, incomplete
}

int check_proposal_state(bcomm_engine_t* eng, int pid){
    make_progress_gen(eng, NULL);
    return eng->my_own_proposal.state;
}

int iar_submit_proposal(bcomm_engine_t* eng, char* proposal, unsigned long prop_size, ID my_proposal_id){

    eng->my_own_proposal.pid = my_proposal_id;
    eng->my_own_proposal.proposal_msg = NULL;
    eng->my_own_proposal.vote = 1;
    eng->my_own_proposal.votes_needed = eng->my_bcomm->send_list_len;
    eng->my_own_proposal.votes_recved = 0;
    eng->my_own_proposal.decision_msg = NULL;
//    printf("%s:%u - rank = %03d, send_list_len = %d\n",
//            __func__, __LINE__, eng->my_bcomm->my_rank, eng->my_bcomm->send_list_len);
    char proposal_send_buf[MSG_SIZE_MAX] = "";
    size_t buf_len;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    if(0 != pbuf_serialize(my_proposal_id, 1, prop_size, proposal, proposal_send_buf, &buf_len)) {
        printf("pbuf_serialize failed.\n");
        return -1;
    }
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    bcomm_GEN_msg_t* proposal_msg = msg_new_bc(eng, proposal_send_buf, buf_len);


//    printf("%s:%u - rank = %03d, needed = %d \n", __func__, __LINE__,
//            eng->my_bcomm->my_rank, eng->my_own_proposal.votes_needed);
    eng->my_own_proposal.state = IN_PROGRESS;
    eng->my_own_proposal.proposal_msg = proposal_msg;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    bcast_gen(eng, proposal_msg, IAR_PROPOSAL);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    make_progress_gen(eng, NULL);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

    if(eng->my_own_proposal.state == COMPLETED)
        return eng->my_own_proposal.vote;//result
    else
        return -1;// not complete
}

int _iar_decision_bcast(bcomm_engine_t* eng, ID my_proposal_id, Vote decision){
    size_t send_len = 0;
    char decision_send_buf[64] = "";
    char debug_info[16] = "IAR_DEC";
    pbuf_serialize(my_proposal_id, decision, strlen(debug_info), debug_info, decision_send_buf, &send_len);
    bcomm_GEN_msg_t* decision_msg = msg_new_bc(eng, decision_send_buf, 64);
    //printf("%s:%u - rank = %03d, new decision msg = %p\n", __func__, __LINE__, eng->my_bcomm->my_rank, decision_msg);
    int my_rank = get_my_rank();
    printf("%s:%u - rank = %03d, strlen(debug_info) = %lu, pid = %d, vote = %d, data_len = %lu, data = [%s]\n", __func__, __LINE__, my_rank,
            strlen(debug_info),
            *((ID*)decision_send_buf), *((Vote*)(decision_send_buf + sizeof(ID))), *((size_t*)(decision_send_buf
                    + sizeof(ID) + sizeof(Vote))), (char*)(decision_send_buf + sizeof(ID) + sizeof(Vote)
                            + sizeof(size_t)));
    PBuf* t = malloc(sizeof(PBuf));
    pbuf_deserialize(decision_send_buf, t);

    printf("%s:%u - rank = %03d, pid = %d, vote = %d, data_len = %lu, data = [%s]\n", __func__, __LINE__, my_rank,
            t->pid, t->vote, t->data_len, t->data);
    bcast_gen(eng, decision_msg, IAR_DECISION);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    eng->my_own_proposal.decision_msg = decision_msg;
    return -1;
}

// Not used any more.
// recv_msg_buf_in_out is the iterator for the queue
int _gen_bc_msg_handler(bcomm_engine_t* eng, bcomm_GEN_msg_t* recv_msg_buf_in) {
    if(!eng || !recv_msg_buf_in)
        return -1;

    if(_test_completed(eng, recv_msg_buf_in)) {

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


user_msg* user_msg_mock(bcomm_GEN_msg_t* gen_msg_in){
    assert(gen_msg_in);
    user_msg* msg_out = (user_msg*)gen_msg_in;

    msg_out->type = gen_msg_in->irecv_stat.MPI_TAG;

//    int my_rank = get_my_rank();
//    printf("Rank %d:  usr_msg content before: type = %d, pid = %d, vote = %d, data = [%s], data_len = %d\n",
//            my_rank, msg_out->type, msg_out->pid, msg_out->vote, msg_out->data, msg_out->data_len);

    if(msg_out->type == IAR_DECISION){
            PBuf* pbuf = malloc(sizeof(PBuf));
            pbuf_deserialize(gen_msg_in->data_buf, pbuf);

            msg_out->pid = pbuf->pid;
            msg_out->vote = pbuf->vote;
            msg_out->data = pbuf->data;
            msg_out->data_len = pbuf->data_len;
            free(pbuf);//to keep msg_out->data valid, Don't use pbuf_free() here!!
    }

//    printf("Rank %d:  usr_msg content after: type = %d, pid = %d, vote = %d, data = [%s], data_len = %d\n",
//            my_rank, msg_out->type, msg_out->pid, msg_out->vote, msg_out->data, msg_out->data_len);

    return msg_out;
}

// Called by the application/user, pickup a msg from the head of the queue.
// Assuming the msg will be copied and stay safe, and will be unlinked from pickup_queue.
// The user should free msg_out when it's done.
// NOTE: if this function is called in a thread different from the progress_engine thread, there will be a thread safe issue.

int user_pickup_next(bcomm_engine_t* eng, user_msg** msg_out) {
    assert(eng);
    bcomm_GEN_msg_t* msg = eng->queue_wait_and_pickup.head;
    if (msg) {        //wait_and_pickup empty

        while (msg) {
            bcomm_GEN_msg_t* msg_t = msg->next;

            if (!msg->pickup_done) { //find a unread msg, mark read, move to wait_fwd queue
                queue_remove(&(eng->queue_wait_and_pickup), msg);
                // mark pickup_done in user_msg_done()
                queue_append(&(eng->queue_wait), msg);
                //printf("%s:%u - rank = %03d, buf = [%s], data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg->msg_usr.buf, msg->msg_usr.data);
                *msg_out = user_msg_mock(msg);

                return 1;
            }
            msg = msg_t;
        }
        //no msg match in this queue. go with next queue.
    }
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    msg = eng->queue_pickup.head;

    if (!(eng->queue_pickup.head)) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        return 0;
    } else { //not empty, find the first available in queue_pickup
        while (msg) {
            bcomm_GEN_msg_t* msg_t = msg->next;

            if (!msg->pickup_done) { //return a msg
                queue_remove(&(eng->queue_pickup), msg);
                // mark pickup_done and free the msg in user_msg_done()
                //printf("%s:%u - rank = %03d, buf = [%s], data = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg->msg_usr.buf, msg->msg_usr.data);
                *msg_out = user_msg_mock(msg);

                return 1;
            }
            msg = msg_t; // next
        }
    }
    return 0;
}

int user_msg_recycle(bcomm_engine_t* eng, user_msg* msg_in){
    assert(eng && msg_in);
    bcomm_GEN_msg_t* msg = (bcomm_GEN_msg_t*) msg_in;
    msg->pickup_done = 1;
    if(msg->fwd_done){
//        printf("%s:%u - rank = %03d, msg_in.post_irecv_type = %d, send_type = %d\n",
//                __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->post_irecv_type, msg_in->send_type);
        msg_free(msg);
        return 1;
    }
    //still in wait queue
    return 0;
}

// Loop through all msgs in the queue, test if all isends are done.
int _wait_and_pickup_queue_process(bcomm_engine_t* eng, bcomm_GEN_msg_t* wait_and_pickup_msg){
    int ret = -1;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
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
    int ret = -1;
    bcomm_GEN_msg_t* cur_wait_only_msg = eng->queue_wait.head;
    while(cur_wait_only_msg){
        bcomm_GEN_msg_t* msg_t = cur_wait_only_msg->next;
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);

        if(msg_test_isends(eng, cur_wait_only_msg)){
            cur_wait_only_msg->fwd_done = 1;
            queue_remove(&(eng->queue_wait), cur_wait_only_msg);

            if(cur_wait_only_msg->send_type == BCAST){//cover bcast and decision, not free when its IAR_PROPOSAL
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                msg_free(cur_wait_only_msg);
            }

            ret = 1;
        }
        cur_wait_only_msg = msg_t;
    }
    return ret;
}

bcomm_GEN_msg_t* _find_proposal_msg(bcomm_engine_t* eng, ID pid){
//    printf("%s:%u - rank = %03d, search pid = %d\n", __func__, __LINE__, eng->my_bcomm->my_rank, pid);
    if(pid < 0)
        return NULL;

    bcomm_GEN_msg_t* msg = eng->queue_iar_pending.head;

    if(!msg)
        return NULL;

    assert(msg->prop_state);

    while(msg){
        if(msg->prop_state->pid == pid){
            return msg;
        }
//
//        printf("%s:%u - rank = %03d: msg = %p, msg->prev = %p, next = %p, prop_state = %p\n",
//                __func__, __LINE__, eng->my_bcomm->my_rank, msg, msg->prev, msg->next, msg->prop_state);

        msg = msg->next;
    }

    return NULL;
}

//Collecting
int _vote_merge(bcomm_engine_t* eng, int pid, Vote vote_in, proposal_state* ps_out){
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    bcomm_GEN_msg_t* msg = _find_proposal_msg(eng, pid);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    if(!msg)
        return -1; //msg not found
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    msg->prop_state->vote &= vote_in;
    msg->prop_state->votes_recved++;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    *ps_out = *(msg->prop_state);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    if(msg->prop_state->votes_needed == msg->prop_state->votes_recved)
        return 1;
    else
        return 0;

    return msg->prop_state->vote;
}

Req_stat _iar_check_status(bcomm_engine_t* eng, int pid){
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    bcomm_GEN_msg_t* msg = _find_proposal_msg(eng, pid);
    if(msg)
        return msg->prop_state->state;
    return INVALID;
}

int _iar_pending_queue_update_status(bcomm_engine_t* eng, int pid, Req_stat stat_in){
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
    bcomm_GEN_msg_t* msg = _find_proposal_msg(eng, pid);
    if(!msg)
        return -1;
    msg->prop_state->state = stat_in;
    return 0;
}

// process queue for received proposals.
// Don't need to do anything???
int _iar_pending_queue_process(bcomm_engine_t* eng, bcomm_GEN_msg_t* decision_msg_in){//store received proposal msgs
    bcomm_GEN_msg_t* msg = eng->queue_iar_pending.head;//all with in_progress status

    assert(msg->prop_state);

    while(msg){
        if(msg->prop_state->pid == decision_msg_in->prop_state->pid){

        }
        msg = msg->next;
    }
    return 0;
}

//msg is a recv_buf in bc_recv_buf_q, and already received data.
//Returns the cnt of sends.
int _bc_forward(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in) {
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
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
        /* Determine which ranks to send to */
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

            if(status.MPI_TAG == BCAST){//bc
                queue_append(&(eng->queue_wait_and_pickup), msg_in);
                eng->fwd_queued++;
            }else{//iar_proposal, decision
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                if(status.MPI_TAG != IAR_DECISION)
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
                    if(status.MPI_TAG == BCAST){
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        queue_append(&(eng->queue_wait_and_pickup), msg_in);
                        eng->fwd_queued++;
                    }else{//iar_proposal, decision
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        if(status.MPI_TAG != IAR_DECISION)
                            queue_append(&(eng->queue_iar_pending), msg_in);
                        eng->fwd_queued++;
                    }

                } else {
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    //printf("%s:%u my rank = %03d, append to queue_pickup queue, msg = %s\n", __func__, __LINE__, eng->my_bcomm->my_rank, msg_in->data_buf);
                    if(status.MPI_TAG == BCAST){
                        msg_in->fwd_done = 1;
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        queue_append(&(eng->queue_pickup), msg_in);
                        eng->fwd_queued++;
                    }else{//iar_proposal, decision
                        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                        if(status.MPI_TAG != IAR_DECISION)
                            queue_append(&(eng->queue_iar_pending), msg_in);
                        eng->fwd_queued++;
                    }
                }
            } /* end if */
            else {
                //printf("%s:%u - rank = %03d Something is wrong... upper_bound = %d, add to pickup queue. msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, upper_bound, msg_in->data_buf);
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                if(status.MPI_TAG == BCAST){
                    msg_in->fwd_done = 1;
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    queue_append(&(eng->queue_pickup), msg_in);
                    eng->fwd_queued++;
                } else {//iar_proposal, decision
                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
                    if(status.MPI_TAG != IAR_DECISION)
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
        if(status.MPI_TAG == BCAST){
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            queue_append(&(eng->queue_pickup), msg_in);
            eng->fwd_queued++;
        }else{
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, eng->my_bcomm->my_rank);
            if(status.MPI_TAG != IAR_DECISION)
                queue_append(&(eng->queue_iar_pending), msg_in);
        }
    }
    return send_cnt;
}

int _iar_process_infra_q_msg(bcomm_GEN_msg_t* msg) {
    return -1;
}

/* ----------------- refactoring for progress engine END ----------------- */
/* ----------------------------------------------------------------------- */
/* ----------------------------------------------------------------------- */

int proposal_state_init(proposal_state* new_prop_state, bcomm_GEN_msg_t* prop_msg_in) {
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
    new_prop_state->state = INVALID;

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
        pools[i].state = INVALID;
    }
    return 0;
}

int pbuf_serialize(ID pid_in, Vote vote, size_t data_len_in, char* data_in,
        char* buf_out, size_t* buf_len_out) {
    assert(buf_out);//assume it's allocated or is a fixed-size array.

    if(data_len_in == 0) {
        if(data_in != NULL)
            return -1;
    }

    if(!buf_out)
        return -1;

    //memcpy(buf_out, &pid_in, sizeof(ID));
    *((ID*)buf_out) = pid_in;
    //memcpy(buf_out + sizeof(ID), &vote, sizeof(Vote));
    *((Vote*)(buf_out + sizeof(ID))) = vote;
    //memcpy(buf_out + sizeof(ID) + sizeof(Vote), &data_len_in, sizeof(unsigned int));
    *((size_t*)(buf_out + sizeof(ID) + sizeof(Vote))) = data_len_in;

    if(data_len_in != 0)
        memcpy(buf_out + sizeof(ID) + sizeof(Vote) + sizeof(size_t), data_in, data_len_in);
    if(buf_len_out) {
        *buf_len_out = sizeof(ID)  /* SN */
            + sizeof(Vote)          /* vote/decision */
            + sizeof(size_t)  /* data_len */
            + data_len_in;          /* data */
    }

    int my_rank = get_my_rank();
    printf("%s:%u - rank = %03d, pid = %d, vote = %d, data_len = %lu, data = [%s]\n", __func__, __LINE__, my_rank,
            *((ID*)buf_out), *((Vote*)(buf_out + sizeof(ID))), *((size_t*)(buf_out + sizeof(ID) + sizeof(Vote))), (char*)(buf_out + sizeof(ID) + sizeof(Vote) + sizeof(size_t)));
    return 0;
}

void pbuf_free(PBuf* pbuf) {
    if(pbuf->data)
        free(pbuf->data);
    free(pbuf);
}

int pbuf_deserialize(char* buf_in, PBuf* pbuf_out) {
    if(!buf_in || !pbuf_out)
        return -1;
    pbuf_out->pid = *((ID*)buf_in);

    pbuf_out->vote = *((Vote*)(buf_in + sizeof(ID)));

    pbuf_out->data_len = *((size_t*)(buf_in + sizeof(ID) + sizeof(Vote)));

    pbuf_out->data = malloc(MSG_SIZE_MAX - sizeof(ID));
    memcpy(pbuf_out->data, buf_in + sizeof(ID) + sizeof(Vote) + sizeof(size_t), pbuf_out->data_len);
    //pbuf_out->data = (char*) buf_in + sizeof(ID) + sizeof(Vote) + sizeof(unsigned int);

    return 0;
}

ID make_pid(bcomm* my_bcomm) {
    return (ID) my_bcomm->my_rank;
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

    my_bcomm->bcast_send_cnt = 0;

//    printf("%s:%u - rank = %03d, level = %d, send_channel_cnt = %d, send_list_len = %d\n",
//            __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_channel_cnt,
//            my_bcomm->send_list_len);
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
                        //printf("%s:%u - rank = %03d: msg (origin_rank %d) can be fwd to rank %d\n", __func__, __LINE__,
                        //        my_bcomm->my_rank, origin_rank, my_bcomm->send_list[i]);
                    }
                }
            } // else: 0
        }
    }
    return send_cnt;
}

int bcast_gen(bcomm_engine_t* eng, bcomm_GEN_msg_t* msg_in, enum COM_TAGS tag) {
    bcomm* my_bcomm = eng->my_bcomm;
    msg_in->bc_init = 1; // just to ensure.
    msg_in->pickup_done = 1; // bc msg doesn't need pickup.

    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--) {
        MPI_Isend(msg_in->msg_usr.buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm,
            &(msg_in->bc_isend_reqs[i]));
        msg_in->send_cnt++;
    }
    // bc has no need of local pickup
//    if(tag != IAR_DECISION){
//        msg_in->send_type = IAR_DECISION;
//        queue_append(&(eng->queue_wait), msg_in);
//    }


    msg_in->send_type = tag;
    queue_append(&(eng->queue_wait), msg_in);



    if(tag != IAR_PROPOSAL){
        eng->sent_bcast_cnt++;
    }
    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    return 0;
}

int engine_cleanup(bcomm_engine_t* eng){
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat_out1;
    int done = 0;
    int my_rank = get_my_rank();
    bcomm_GEN_msg_t* recv_msg;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
    MPI_Iallreduce(&(eng->sent_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, eng->my_bcomm->my_comm, &req);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
    do {
        MPI_Test(&req, &done, &stat_out1);// test for MPI_Iallreduce.
        if (!done) {
            make_progress_gen(eng, &recv_msg);
        }
    } while (!done);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);

    // Core cleanup section
    while (eng->recved_bcast_cnt + eng->sent_bcast_cnt < total_bcast) {
        make_progress_gen(eng, &recv_msg);
    }
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
    recv_msg = NULL;
    user_msg* pickup_out = NULL;
    while(user_pickup_next(eng, &pickup_out)){
        total_pickup++;
        //printf("%s:%u - rank = %03d, pickup_out msg = [%s]\n", __func__, __LINE__, eng->my_bcomm->my_rank, pickup_out->data_buf);
    }

    free(eng->my_bcomm);
    free(eng);
    return 0;
}

int proposal_reset(proposal_state* ps){
    assert(ps);
    if(ps->decision_msg)
        msg_free(ps->decision_msg);
    ps->decision_msg = NULL;
    if(ps->proposal_msg)
        msg_free(ps->proposal_msg);
    ps->proposal_msg = NULL;

    ps->pid = -1;
    ps->recv_proposal_from = -1;
    ps->state = INVALID;
    ps->vote = -1;
    ps->votes_needed = 0;
    ps->votes_recved = 0;

    return 0;
}

int get_vote_my_proposal(bcomm_engine_t* eng){
    if(eng->my_own_proposal.state != COMPLETED){
        return -1;
    }
    int ret = eng->my_own_proposal.vote;
    proposal_reset(&(eng->my_own_proposal));
    return ret;
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

int queue_test(int cnt){
    queue q;
    q.head = NULL;
    q.tail = NULL;
    q.msg_cnt = 0;

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
        printf("Looping queue after appending: msg->id_debug = %d\n", cur->id_debug);
        cur = cur->next;
    }

    cur = q.head;
    printf("cur = %p, q.cnt = %d\n", cur, q.msg_cnt);
    while(cur){
        printf("Remove element: msg->id_debug = %d\n", cur->id_debug);
        bcomm_GEN_msg_t* t = cur->next;
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
