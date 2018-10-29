#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>
#include <stdbool.h>
#include <pthread.h>

#define MSG_SIZE_MAX 32768
#define VOTE_POOL_SIZE 16 //maximal concurrent proposal supported
enum COM_TAGS {
    BCAST,
    JOB_DONE,
    IAR_PROPOSAL,
    IAR_VOTE, /* vote for I_All_Reduce */
    IAR_DECISION, /* Final result */

};

typedef int ID;
typedef int Vote;// used for & operation. 1 for yes, 0 for no.
typedef struct Proposal_pool{
    ID pid; // proposal ID, default = -1;
    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
//    int proposal_sent_cnt;
    Vote vote; //accumulated vote, default = 1;
    int votes_needed; //num of votes needed to report to parent, equals to the number of sends of a proposal
    int votes_recved;
} proposal_pool; //clear when vote result is reported.

int proposal_compete(char* p1, char* p2);

int proposalPool_init(proposal_pool* pp_in_out){
    if(!pp_in_out)
        return -1;

    pp_in_out->pid = -1;
//    pp_in_out->proposal_sent_cnt = -1;
    pp_in_out->recv_proposal_from = -1;
    pp_in_out->vote = 1;
    pp_in_out->votes_needed = -1;
    pp_in_out->votes_recved = -1;
    return 0;
}

//return index, or error.
int proposalPool_proposal_add(proposal_pool* pools, proposal_pool* pp_in){//add new, or merge value
    if(!pools || !pp_in)
        return -1;

    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){//exists, merge
        if(pools[i].pid == pp_in->pid){
            printf("Function %s:%u - find proposal, pid = %d , index = %d\n", __func__, __LINE__, pp_in->pid, i);
            pools[i] = *pp_in;
            printf("Function %s:%u - confirm in array, pid = %d , index = %d\n", __func__, __LINE__, pools[i].pid, i);
            return i;
        }
    }

    // id not found, add new one.

    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){

        if(pools[i].pid == -1){//first available

            pools[i]= *pp_in;
            printf("Function %s:%u - pp added, confirm in array, pid = %d , index = %d\n", __func__, __LINE__, pools[i].pid, i);
            return i;
        }
    }

    if(i == VOTE_POOL_SIZE - 1) //no empty pool for use.
        return -1;


    return -2;
}

//update vote_needed field, which is set by _forward(), equals # of sends done on a proposal.
int proposalPool_proposal_setNeededVoteCnt(proposal_pool* pools, ID k, int cnt){
    if(!pools)
        return -1;

    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){//must exists, this happened after a proposal is added by _IAR_process()
        if(pools[i].pid == k){
            pools[i].votes_needed = cnt;
            return 0;
        }
    }
    return -1;
}

int proposalPool_vote_merge(proposal_pool* pools, ID k, Vote v){
    if(!pools)
        return -1;

    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){//exists, merge
        if(pools[i].pid == k){
            pools[i].votes_recved++;
            pools[i].vote &= v;
            return 0;
        }
    }
    // id not found, need to add new one, not just merge
    return -2;
}

int proposalPool_get(proposal_pool* pools, ID k, proposal_pool* result){
    if(!result)//null
        return -1;
    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){
        if(pools[i].pid == k){
            *result = pools[i];
            return 0;
        }
    }
    return -1;//not found
}

int proposalPool_get_index(proposal_pool* pools, ID k){
    if(!pools)//null
        return -1;
    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){
        if(pools[i].pid == k)
            return i;
    }
    return -1;//not found
}

int proposalPool_rm(proposal_pool* pools, ID k){
    int i = 0;
    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){
        if(pools[i].pid == k){
            pools[i].pid = -1;
            pools[i].vote = 1;
            return 0;
        }
    }
    return -1;//not found
}

int proposalPools_reset(proposal_pool* pools) {
    for (int i = 0; i <= VOTE_POOL_SIZE - 1; i++) {
        pools[i].pid = -1;
        pools[i].vote = 1;
        pools[i].recv_proposal_from = -1;
        pools[i].votes_needed = -1;
        pools[i].votes_recved = -1;
    }
    return 0;
}

//int proposalPool_vote_incre(proposal_pool* pools, ID k){//included in vote_merge
//    int i = 0;
//    for(i = 0; i <= VOTE_POOL_SIZE - 1; i++){
//        if(pools[i].pid == k){
//            if(pools[i].vote == -1 || pools[i].vote == 0){
//                pools[i].vote = 1;
//            }else
//                pools[i].vote++;
//            return 0;
//        }
//    }
//    return -1;//not found
//}


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

    /* Skip ring fields */
    int my_level;                       /* Level of rank in ring */
    int last_wall;                      /* The closest rank that has higher level than rank world_size - 1 */
    int world_is_power_of_2;            /* Whether the world size is a power of 2 */

    /* Send fields */
    int send_channel_cnt;               /* # of outgoing channels from this rank */
    int send_list_len;                  /* # of outgoing ranks to send to */
    int* send_list;                     /* Array of outgoing ranks to send to */
    void *send_buf;                     /* Buffer for sending messages */
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
    proposal_pool my_own_proposal;      /* Set only when I'm a IAR starter, maintain status for my own proposal */
    proposal_pool my_proposal_pools[VOTE_POOL_SIZE];        /* To support multiple proposals, use a vote pool for each proposal. Use linked list if concurrent proposal number is large. */
    char* my_proposal;                  /* This is used to compare against received proposal, shuold be updated timely */
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

char DEBUG_MODE = 'O';
typedef struct {
    FILE* log_file;
    int my_rank;
} Log;
Log MY_LOG;

//IAR msg formats
// | SN pid | char* proposal_content |
typedef struct Proposal_buf{
    ID pid;
    Vote vote;//vote or decision
    unsigned int data_len;
    char* data;
}PBuf;

int pbuf_serialize(ID pid_in, Vote vote, unsigned int data_len_in, char* data_in, char* buf_out, unsigned int* buf_len_out){
    if(data_len_in == 0){
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
    if(buf_len_out){
        *buf_len_out = sizeof(ID)  /* SN */
            + sizeof(Vote)          /* vote/decision */
            + sizeof(unsigned int)  /* data_len */
            + data_len_in;          /* data */
    }
    return 0;
}

void pbuf_free(PBuf* pbuf){
    free(pbuf->data);
    free(pbuf);
}

int pbuf_deserialize(char* buf_in, PBuf* pbuf_out){
    if(!buf_in || !pbuf_out)
        return -1;
    memcpy(&(pbuf_out->pid), buf_in, sizeof(ID));
    memcpy(&(pbuf_out->vote), buf_in + sizeof(ID), sizeof(Vote));
    memcpy(&(pbuf_out->data_len), buf_in + sizeof(ID) + sizeof(Vote), sizeof(unsigned int));
    //printf("%s: pid = %d, data_len = %u\n", __func__, pbuf_out->pid, pbuf_out->data_len);
    //printf("%s:%u \n", __func__, __LINE__);
    pbuf_out->data = malloc(MSG_SIZE_MAX - sizeof(ID));
    //printf("%s:%u \n", __func__, __LINE__);
    memcpy(pbuf_out->data, buf_in + sizeof(ID) + sizeof(Vote) + sizeof(unsigned int), pbuf_out->data_len);
    return 0;
}

//int pbuf_deserialize_t(char* buf_in, ID* pid_out, unsigned int* data_len_out, char* data_out){
//    if(!buf_in)
//        return -1;
//
//    if(pid_out)
//        memcpy(pid_out, buf_in, sizeof(ID));
//
//    unsigned int len = 0;
//    memcpy(&len, buf_in + sizeof(ID), sizeof(unsigned int));
//
//    if(data_len_out)
//        *data_len_out = len;
//
//    if(data_out)
//        memcpy(data_out, buf_in + sizeof(ID) + sizeof(unsigned int), len);
//    return 0;
//}

typedef struct Vote_buf{
    ID pid;
    Vote vote;
}VBuf;

ID make_pid(bcomm* my_bcomm){
    return (ID) my_bcomm->my_rank;
}

int proposal_agree(char* p1, char* p2);

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

void log_init(MPI_Comm my_comm, Log my_log) {
    char fname[128];
    MPI_Comm_rank(my_comm, &(my_log.my_rank));
    char time_str[64];
    get_time_str(time_str);
    sprintf(fname, "Log_rank_%03d_%s.log", my_log.my_rank, time_str);
    my_log.log_file = fopen(fname, "w+");
}

void log_close(Log my_log) {
    if (DEBUG_MODE == 'F') {
        fflush(my_log.log_file);
        fclose(my_log.log_file);
    }
    return;
}

void debug(Log my_log) {
    if (DEBUG_MODE == 'O') {
        return;
    }

    char log_line[MSG_SIZE_MAX];
    sprintf(log_line, "%s:%u - rank = %03d\n", __func__, __LINE__, my_log.my_rank);
    switch (DEBUG_MODE) {
    case 'P':
        printf("%s\n", log_line);
        break;
    case 'F':
        printf("fputs...\n");
        fputs(log_line, my_log.log_file);
        break;
    default:
        break;
    }
    return;
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
    my_bcomm->msg_size_max = msg_size_max;

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
    my_bcomm->fwd_isend_reqs[0] = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    my_bcomm->fwd_isend_reqs[1] = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    my_bcomm->fwd_isend_stats[0] = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    my_bcomm->fwd_isend_stats[1] = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    my_bcomm->bcast_send_cnt = 0;
    my_bcomm->bcast_isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    my_bcomm->bcast_isend_stats = malloc(my_bcomm->send_list_len * sizeof(MPI_Status));
    my_bcomm->send_buf = malloc(sizeof(int) + msg_size_max);
    memcpy(my_bcomm->send_buf, &my_bcomm->my_rank, sizeof(int));
    my_bcomm->user_send_buf = ((char *)my_bcomm->send_buf) + sizeof(int);

    /* Set up receive fields */
    my_bcomm->recv_buf[0] = (char*) malloc(sizeof(int) + msg_size_max);
    my_bcomm->recv_buf[1] = (char*) malloc(sizeof(int) + msg_size_max);
    my_bcomm->curr_recv_buf_index = 0;

    /* Set up I_All_Reduce fields */
    my_bcomm->IAR_recv_buf = (char*)malloc(sizeof(int) + sizeof(ID) + msg_size_max);                 /* IallReduce recv buf */

    //my_bcomm->vote_my_proposal_no_use = 1;    /* 1 for agree, 0 for decline, default is 1 */
    my_bcomm->IAR_active = 0;
    proposalPools_reset(my_bcomm->my_proposal_pools);

    my_bcomm->my_proposal = (char*)malloc(msg_size_max);                  /* This is used to compare against received proposal, shuold be updated timely */
    my_bcomm->send_buf_my_vote = (char*)malloc(sizeof(int) + sizeof(ID) + sizeof(Vote));
    my_bcomm->recv_vote_cnt = 0;
    //my_bcomm->recv_proposal_from = -1;
    //my_bcomm->proposal_sent_cnt = -1;

    /* Post initial receive for this rank */
    MPI_Irecv(my_bcomm->recv_buf[0], msg_size_max + sizeof(int), MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, my_bcomm->my_comm, &(my_bcomm->irecv_req));

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
    return -1;
}

int bcast(bcomm* my_bcomm, enum COM_TAGS tag);

int _IAllReduce_StarterVote(bcomm* my_bcomm, Vote vote_in, ID pid) {
    //PBuf* vote_buf = malloc(sizeof(PBuf));
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    int pp_index = proposalPool_get_index(my_bcomm->my_proposal_pools, pid);
    printf("%s:%u - rank = %03d: starter send vote %d: index = %d, send to rank %d\n", __func__, __LINE__, my_bcomm->my_rank, vote_in, pp_index, my_bcomm->my_proposal_pools[pp_index].recv_proposal_from);
    if(-1 == pp_index){
        return -1;
    }
//    vote_buf->data_len = 0;
//    vote_buf->data = NULL;//malloc(vote_buf->data_len);
//    memcpy(vote_buf->data, &vote_in, sizeof(Vote));
//    vote_buf->pid = pid;

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    unsigned int send_len;
    pbuf_serialize(pid, vote_in, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->my_proposal_pools[pp_index].recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    bufer_maintain_irecv(my_bcomm);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    //pbuf_free(vote_buf);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    return 0;
}
//Decision format: ID:Vote:Content.
//TODO: need to refactor to reuse PBuf.
int _IAllReduce_bacast_decision(bcomm* my_bcomm, ID pid, char* proposal, int pp_len, Vote decision){
    //memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(unsigned int), &my_bcomm->my_own_proposal.vote, sizeof(Vote));
    if(!proposal){
        if(decision == 1)
            return -1;
    }
    memcpy(my_bcomm->user_send_buf, &pid, sizeof(ID));
    memcpy(my_bcomm->user_send_buf + sizeof(ID), &decision, sizeof(Vote));

    if(decision == 1){
        memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(Vote), proposal, pp_len);
    }

    return bcast(my_bcomm, IAR_DECISION);
}
int _IAllReduce_process(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out){
    // All msg that without a proposal or vote tag (such as decision) will cause return 0 and captured by _forward().
    printf("%s:%u - rank = %03d, status.MPI_TAG = %d, status.source = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG, status.MPI_SOURCE);

    if(status.MPI_TAG == IAR_VOTE){//collect vote and up stream
        //  - If it's a vote for me (only when its IAR_active), only merge, but no upstreaming
        //  - else
        //      - if non-leaf rank: merge and wait until all votes collected, then up stream
        //      - if it is a leaf rank, vote back directly.
        printf("%s:%u - rank = %03d, received a vote from rank %d.\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE);
        if(recv_buf_out)// not NULL
            *recv_buf_out = (char *) my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];

        //my_bcomm->recv_vote_cnt ++;

        PBuf* vote_buf = malloc(sizeof(PBuf));
        pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index], vote_buf);// + sizeof(int). IAR_VOTE doepid't need origin

        if(my_bcomm->IAR_active == 1){//If it's a vote for my proposal (only when its IAR_active), only merge, but no upstreaming
            if(vote_buf->pid == my_bcomm->my_own_proposal.pid){
                printf("%s:%u - rank = %03d, received a vote from rank %d for my proposal, vote = %d.\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, vote_buf->vote);
                my_bcomm->my_own_proposal.votes_recved++;
                my_bcomm->my_own_proposal.vote &= vote_buf->vote;//*(Vote*)(vote_buf->data);
                if(my_bcomm->my_own_proposal.votes_recved == my_bcomm->my_own_proposal.votes_needed){//all done, bcast decision.
                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(vote_buf);
                    return 5;
                } else {// need more votes for my decision, continue to irecv.
                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(vote_buf);
                    return 4;
                }
            }
            //not for me: same as non-starter
        }

        //votes not for me....
        proposal_pool pp;
        //check if exist, if yes, increase it by merging. if no, it's an error, since a proposal_pool should exist before a corresponding vote arrive.
        if(0 != proposalPool_vote_merge(my_bcomm->my_proposal_pools, vote_buf->pid, vote_buf->vote)){
            printf("Function %s:%u - rank %d: can't merge vote, proposal not exists, pid = %d \n", __func__, __LINE__, my_bcomm->my_rank, vote_buf->pid);
            bufer_maintain_irecv(my_bcomm);
            pbuf_free(vote_buf);
            return -1;
        }
        int p_index = proposalPool_get_index(my_bcomm->my_proposal_pools, vote_buf->pid);
        printf("Function %s:%u - rank %d: completed merging vote, pid = %d, votes_needed = %d, votes_recved = %d \n", __func__, __LINE__, my_bcomm->my_rank, vote_buf->pid, my_bcomm->my_proposal_pools[p_index].votes_needed, my_bcomm->my_proposal_pools[p_index].votes_recved);

        if(-1 == p_index){
            printf("Function %s:%u - rank %d: can't find proposal. \n", __func__, __LINE__, my_bcomm->my_rank);
            bufer_maintain_irecv(my_bcomm);
            pbuf_free(vote_buf);
            return -1;
        }

        //printf("%s:%u - rank %03d received a vote: %d, now my vote = %d\n", __func__, __LINE__, my_bcomm->my_rank, *(Vote*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(SN)+ sizeof(unsigned int)), my_bcomm->my_vote);
        if(my_bcomm->my_proposal_pools[p_index].votes_recved == my_bcomm->my_proposal_pools[p_index].votes_needed){//all votes are received, report to predecessor
            printf("Function %s:%u - rank %d: all votes (%d) are received, report to predecessor rank %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_proposal_pools[p_index].votes_needed, my_bcomm->my_proposal_pools[p_index].recv_proposal_from);
            vote_buf->vote = my_bcomm->my_proposal_pools[p_index].vote;
            unsigned int send_len;
            pbuf_serialize(vote_buf->pid, vote_buf->vote, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
            MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->my_proposal_pools[p_index].recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
            bufer_maintain_irecv(my_bcomm);
            pbuf_free(vote_buf);
            return 3;//received all votes and reported to parent, continue
        }

        bufer_maintain_irecv(my_bcomm);
        pbuf_free(vote_buf);
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        return 2;//received a vote, but expecting more
    }// end vote handling

    if(status.MPI_TAG == IAR_PROPOSAL){// new proposal, down stream
        if(recv_buf_out)// not NULL
            *recv_buf_out = (char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]+ sizeof(int);

        PBuf* pbuf = malloc(sizeof(PBuf));
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);

        int origin = get_origin((char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]);
        printf("%s:%u - rank = %03d, received a proposal from rank %d:(%d:%s)\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->data);
        //my_bcomm->recv_proposal_from = status.MPI_SOURCE;
        //my_bcomm->proposal_sent_cnt = 0;


        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        if (0 == proposal_agree(my_bcomm->my_proposal, pbuf->data)) {//local declined, up stream to the parent, no need of collecting votes.
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            //set vote
            Vote tmp_v = 0;
            //my_bcomm->vote_my_proposal_no_use = tmp_v;
            //proposalPool_vote_merge(my_bcomm->my_vote_pools, pbuf->pid, 0);

            unsigned int send_len;
            pbuf_serialize(pbuf->pid, tmp_v, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);

            MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, status.MPI_SOURCE, IAR_VOTE, my_bcomm->my_comm);// sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

            bufer_maintain_irecv(my_bcomm);
            pbuf_free(pbuf);
            return 1;//proposal declined locally, reported, continue
        }else{//local approved

            if(my_bcomm->IAR_active == 0){//Not an IAR starter, downstream/forward if not a leaf rank.
                memcpy(my_bcomm->send_buf_my_vote, my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), sizeof(ID));//copy pid

                Vote tmp_v = 1;
                //my_bcomm->vote_my_proposal_no_use = tmp_v;
                proposal_pool pp;
                proposalPool_init(&pp);
                pp.pid = pbuf->pid;
                pp.recv_proposal_from = status.MPI_SOURCE;
                pp.votes_recved = 0;
                pp.vote = 1;

                //non-leaf rank need to report after collecting all children votes, handled by IAR_VOTE tag branch.
                printf("%s:%u - rank = %03d: adding pp...\n", __func__, __LINE__, my_bcomm->my_rank);
                int paindex = proposalPool_proposal_add(my_bcomm->my_proposal_pools, &pp);
                printf("%s:%u - rank = %03d: getting pp index...\n", __func__, __LINE__, my_bcomm->my_rank);
                int pindex = proposalPool_get_index(my_bcomm->my_proposal_pools,pbuf->pid);
                printf("%s:%u - rank = %03d, non-starter, non-leaf rank added a propolsal, pid = %d , paindex = %d, pindex = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_proposal_pools[pindex].pid, paindex, pindex);

                // //leaf rank don't report until all children's votes are received, or if it's a leaf rank, reports directly.
                if(my_bcomm->send_channel_cnt == 0){//leaf rank
                    printf("%s:%u - rank = %03d: leaf rank sending vote...\n", __func__, __LINE__, my_bcomm->my_rank);
                    unsigned int send_len;
                    pbuf_serialize(*(ID*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int)), tmp_v, 0, NULL, my_bcomm->send_buf_my_vote, &send_len);
                    MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, status.MPI_SOURCE, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
                    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(pbuf);
                    return 4;//leaf rank agreed, no forward, continue.
                }else{//non leaf rank, non starter, always forward proposals and decisions
                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(pbuf);
                    printf("%s:%u - rank = %03d: non-leaf forwarding proposal\n", __func__, __LINE__, my_bcomm->my_rank);
                    return 0;
                }

            } else { // starter, all ranks.
                int winner = proposal_compete(my_bcomm->my_proposal, pbuf->data);
                if(winner == 0){// I win, vote no to others.
                    printf("%s:%u - rank = %03d: starter: I win, vote no to others.\n", __func__, __LINE__, my_bcomm->my_rank);
                    _IAllReduce_StarterVote(my_bcomm, 0, pbuf->pid);
                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(pbuf);
                    return 1;
                } else {// others' win, forward if I'm not a leaf

                    //TODO: bcast no decision for my proposal, return 6, but still need to continue forwarding.
                    my_bcomm->my_own_proposal.vote = 0;
                    my_bcomm->my_own_proposal.votes_recved = my_bcomm->my_own_proposal.votes_needed; //condition to end loop
                    //_IAllReduce_bacast_decision(my_bcomm, my_bcomm->my_own_proposal.pid, NULL, 0, 0);


                    // add proposal
                    proposal_pool pp;
                    proposalPool_init(&pp);
                    pp.pid = pbuf->pid;
                    pp.recv_proposal_from = status.MPI_SOURCE;
                    pp.votes_recved = 0;
                    pp.vote = 1;


                    //non-leaf rank need to report after collecting all children votes, handled by IAR_VOTE tag branch.
                    proposalPool_proposal_add(my_bcomm->my_proposal_pools, &pp);

                    int pindex = proposalPool_get_index(my_bcomm->my_proposal_pools,pbuf->pid);
                    printf("%s:%u - rank = %03d, non-starter, non-leaf rank added a propolsal, pid = %d \n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_proposal_pools[pindex].pid);

                    if(my_bcomm->send_channel_cnt != 0){//non leaf, forward

                        if(check_passed_origin(my_bcomm, origin, my_bcomm->send_list[0]) == 0){//pass origin, this is a temperory leaf rank
                            printf("%s:%u - rank = %03d: starter: others' win, non-leaf forwarding...\n", __func__, __LINE__, my_bcomm->my_rank);
                            bufer_maintain_irecv(my_bcomm);
                            pbuf_free(pbuf);
                            return 0;// others' proposal will be forwarded. eturn 0 will be caught by _forward();
                        } else {
                            printf("%s:%u - rank = %03d: starter: others' win, non-leaf can't forward, return 9\n", __func__, __LINE__, my_bcomm->my_rank);
                            bufer_maintain_irecv(my_bcomm);
                            pbuf_free(pbuf);
                            return 9;//unknown error
                        }
                    }else{//leaf, no forward, but need to vote
                        printf("%s:%u - rank = %03d: starter: others' win, leaf rank, no forward. voting yes.\n", __func__, __LINE__, my_bcomm->my_rank);

                        if(0 != _IAllReduce_StarterVote(my_bcomm, 1, pbuf->pid)){
                            bufer_maintain_irecv(my_bcomm);
                            pbuf_free(pbuf);
                            return 9;//unknown error
                        }
                        bufer_maintain_irecv(my_bcomm);
                        pbuf_free(pbuf);
                        return 4;
                    }
                }
            }
        }
        // else case: regular forward
    }
    //neither proposal nor vote.
    return 0;
}

int _forward(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out) {
    printf("%s:%u - rank = %03d, tag = %d \n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG);
    void *recv_buf;

    /* Increment # of messages received */
    my_bcomm->bcast_recv_cnt++;

    /* Set buffer that message was received in */
    recv_buf = my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];
    PBuf* pbuf = malloc(sizeof(PBuf));
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);
    //if(status.MPI_TAG == 2)
        printf("%s:%u - rank = %03d, before forward from rank %d:(%d:%d:%s), tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, pbuf->pid, pbuf->vote, pbuf->data, status.MPI_TAG);
    /* Check for a rank that can forward messages */
    if (my_bcomm->my_level > 0) {
        int origin;
        int send_cnt;

        /* Retrieve message's origin rank */
        origin = get_origin(recv_buf);

        /* Determine which ranks to send to */
        send_cnt = 0;
        if (status.MPI_SOURCE > my_bcomm->last_wall) {
            /* Send messages, to further ranks first */
            for (int j = my_bcomm->send_channel_cnt; j >= 0; j--) {
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
                    if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
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

        if(status.MPI_TAG == IAR_PROPOSAL){
            PBuf* pbuf = malloc(sizeof(PBuf));
            pbuf_deserialize(recv_buf + sizeof(int), pbuf);

            int pp_index =  proposalPool_get_index(my_bcomm->my_proposal_pools, pbuf->pid);
            if(-1 == pp_index){
                return -1;
            }
            my_bcomm->my_proposal_pools[pp_index].votes_needed = send_cnt;

        }

//        if (my_bcomm->proposal_sent_cnt == 0)
//            my_bcomm->proposal_sent_cnt = send_cnt;



    } /* end if */

    /* Return pointer to user data in current receive buffer */
    if (recv_buf_out) // not NULL
        *recv_buf_out = ((char *) recv_buf) + sizeof(int);

    bufer_maintain_irecv(my_bcomm);
    return 0;
}
int make_progress(bcomm* my_bcomm, MPI_Status status, char** recv_buf_out){
    printf("%s:%u - rank = %03d, status.tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG);
    int ret = _IAllReduce_process(my_bcomm, status, recv_buf_out);
    if(ret != 0)
        return ret;

    //BCAST and DECISION tags are treated in the same way.
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    _forward(my_bcomm, status, recv_buf_out);
    return 0;
}

int irecv(bcomm* my_bcomm, char** recv_buf_out, int* recved_tag_out){
    MPI_Status status;
    int completed = 0;
    MPI_Test(&my_bcomm->irecv_req, &completed, &status);

    if(completed){
        if(recved_tag_out)
            *recved_tag_out = status.MPI_TAG;

        //printf("%s:%u - rank = %03d, complete, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_TAG, status.MPI_SOURCE, status.MPI_ERROR);
        if(status.MPI_TAG == -1){

            //printf("%s:%u - rank = %03d, complete, my_bcomm->irecv_req = %d, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->irecv_req, status.MPI_TAG, status.MPI_SOURCE, status.MPI_ERROR);

            return -1;
        }
        //printf("%s:%u - rank = %03d, complete, my_bcomm->irecv_req = %d, tag = %d, source = %d, MPI_ERROR = %d\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->irecv_req, status.MPI_TAG, status.MPI_SOURCE, status.MPI_ERROR);
        return make_progress(my_bcomm, status, recv_buf_out);
    }
    return -1;//not complete
}

int load_bcast(bcomm* my_bcomm){
    return 0;
}
// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_bcomm, enum COM_TAGS tag) {
    /* If there are outstanding messages being broadcast, wait for them now */
    if(my_bcomm->bcast_send_cnt > 0) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
        my_bcomm->bcast_send_cnt = 0;
    } /* end if */
    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--){
        MPI_Isend(my_bcomm->send_buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm,
                &my_bcomm->bcast_isend_reqs[i]);// my_bcomm->my_comm
    }
    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    return 0;
}

int bcomm_teardown(bcomm* my_bcomm) {
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat;
    int done;
    char *recv_buf;
    int recv_cnt;

    /* If there are outstanding messages being broadcast, wait for them now */
    if(my_bcomm->bcast_send_cnt > 0) {
        MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
        my_bcomm->bcast_send_cnt = 0;
    } /* end if */

    /* Retrieve the # of broadcasts from all ranks */
    MPI_Iallreduce(&(my_bcomm->my_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, my_bcomm->my_comm, &req);

    /* Forward messages until all ranks have participated in allreduce for total braoadcast count */
    do {
        MPI_Test(&req, &done, &stat);
        if (!done){
            //recv_forward(my_bcomm, &recv_buf, NULL);
            irecv(my_bcomm, &recv_buf, NULL);
        }
    } while (!done);

    /* Forward messages until we've received all the broadcasts */
    while (my_bcomm->bcast_recv_cnt + my_bcomm->my_bcast_cnt < total_bcast){
        //recv_forward(my_bcomm, &recv_buf, NULL);
        irecv(my_bcomm, &recv_buf, NULL);
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
int proposal_agree(char* p1, char* p2){
    return 1; //for test multi proposal
//    int ret = strcmp(p1, p2);
//    if(ret == 0)
//        return 1;
//    else
//        return 0;
}

//Return 0 if p1 wins, otherwise return 1.
int proposal_compete(char* p1, char* p2){
    if(p1[0] >= p2[0]){
        return 1;//0
    }else
        return 0;//1
}

int iAllReduceStart(bcomm* my_bcomm, char* my_proposal, unsigned long prop_size, ID my_proposal_id){

    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    char* recv_buf = malloc(2* MSG_SIZE_MAX);
    //SN pid = 1234;//make_pid(my_bcomm);

//    proposal_pool pp;
//    proposalPool_init(&pp);
//    pp.pid = my_proposal_id;
//    pp.votes_recved = 0;
//    proposalPool_proposal_add(my_bcomm->my_proposal_pools, &pp);
//    my_bcomm->my_proposal = my_proposal;
//    int pp_index = proposalPool_get_index(my_bcomm->my_proposal_pools, my_proposal_id);


    int recv_vote_cnt = 0;
    int recved_tag = 0;
    int decision_cnt = 0;
    //Vote votes_result = 1;
    Vote recv_vote = 1;
    int ret = -1;
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

    my_bcomm->my_proposal = my_proposal;
    my_bcomm->my_own_proposal.pid = my_proposal_id;
    my_bcomm->my_own_proposal.vote =1;
    my_bcomm->my_own_proposal.votes_needed = my_bcomm->send_list_len;
    my_bcomm->my_own_proposal.votes_recved = 0;
    my_bcomm->IAR_active = 1;

    unsigned buf_len;
    if(0 != pbuf_serialize(my_bcomm->my_own_proposal.pid, my_bcomm->my_own_proposal.vote, prop_size, my_bcomm->my_proposal, my_bcomm->user_send_buf, &buf_len)){
        printf("pbuf_serialize failed.\n");
        return -1;
    }
    bcast(my_bcomm, IAR_PROPOSAL);//IAR_PROPOSAL
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    //use send_list_len ONLY on the started, other places use proposal_sent_cnt.
    //my_bcomm->my_own_proposal.votes_recved < my_bcomm->send_list_len
    while ( my_bcomm->my_own_proposal.votes_recved < my_bcomm->send_list_len ) {// loop until decision is ready for bcast.
        ret = irecv(my_bcomm, &recv_buf, &recved_tag);
        //if(ret == 5)
            //break;

        if(recved_tag == IAR_DECISION){
            decision_cnt++;
        }
        if(ret != -1)
            printf("%s:%u - rank = %03d, starter: irecv() = %d\n", __func__, __LINE__, my_bcomm->my_rank, ret);
        ID other_id = *(ID*)recv_buf;
        //ret == 3: all votes are received
        if(ret == 3 || ret == 2 || ret == 1){//received all votes for my proposals: note that vote won't be forwarded, so they all reach the destination directly.
            recv_vote_cnt++;

            //votes_result &= my_bcomm->vote_my_proposal_no_use;// nouse
            //vote_pool_set(my_bcomm->my_vote_pools, other_id, ) no need, it's updated by irecv.

            printf("%s:%u - rank = %03d, received a vote, recv_vote_cnt = %d, need %d in total.\n", __func__, __LINE__, my_bcomm->my_rank, my_bcomm->my_own_proposal.votes_recved, my_bcomm->my_own_proposal.votes_needed);
        }
        if ( ret == 0 || ret == 4) {//make_progress() == 0, received and forwarded something: proposal or decision, won't be a vote.
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//            if (recved_tag == IAR_PROPOSAL) {
//                // TODO: multi proposal cases
//                printf("%s:%u - rank = %03d, I, a starter, received a proposal. \n", __func__, __LINE__, my_bcomm->my_rank);
//                if(1 == proposal_compete(my_proposal, recv_buf)){//others wins
//                    //vote yes
//
//                    printf("%s:%u - rank = %03d, I'm canceling my own proposal due to a conflicting proposal, vote yes to another proposal(ID = %d)...\n", __func__, __LINE__, my_bcomm->my_rank, other_id);
//                    _IAllReduce_StarterVote(my_bcomm, 1, other_id);
//                    //bcast NO decision for my proposal
//                    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//                    Vote v = 0;
//                    memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(unsigned int), &v, sizeof(Vote));
//                    bcast(my_bcomm, IAR_DECISION);
//
//                    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
//                    //free(recv_buf);??
//                    return 0;
//                }else{//mine wins
//                    //decline received proposal: vote NO.
//
//                    _IAllReduce_StarterVote(my_bcomm, 0, other_id);
//                    printf("%s:%u - rank = %03d, my proposal has canceled a conflicting proposal, vote no to another proposal(ID = %d)...\n", __func__, __LINE__, my_bcomm->my_rank, other_id);
//
//                    //nothing for local proposal
//                }
//            }
            //if (recved_tag == IAR_VOTE) // will never run into here

        }
    }
    //packing a decision: pid:vote:proposal
    //if vote is 0, proposal is empty.
    my_bcomm->IAR_active = 0;
    printf("%s:%u - rank = %03d, Starter bcasting final decision (ID:decision): %d:%d\n", __func__, __LINE__, my_bcomm->my_rank, my_proposal_id, my_bcomm->my_own_proposal.vote);
    memcpy(my_bcomm->user_send_buf + sizeof(ID) + sizeof(unsigned int), &my_bcomm->my_own_proposal.vote, sizeof(Vote));
    unsigned int send_buf_len;
    pbuf_serialize(my_bcomm->my_own_proposal.pid, my_bcomm->my_own_proposal.vote, prop_size, my_bcomm->my_proposal, my_bcomm->user_send_buf, &send_buf_len);
    bcast(my_bcomm, IAR_DECISION);
    printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    //free(recv_buf);??
    return my_bcomm->my_own_proposal.vote;
}

int test_IAllReduce_single_proposal(bcomm* my_bcomm, int starter, int no_rank) {
    char* my_proposal = "111";
    char* recv_buf = malloc(2 * MSG_SIZE_MAX);
    int result = -1;
    //printf("Rank %d: send_channel_cnt = %d, send_list_len = %d\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, my_bcomm->send_list_len);
    if (my_bcomm->my_rank == starter) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        int len = strlen(my_proposal);
        result = iAllReduceStart(my_bcomm, my_proposal, len, my_bcomm->my_rank);

    } else {
        usleep(1500);
        if (my_bcomm->my_rank  == no_rank) {
            my_bcomm->my_proposal = "000";
        } else {
            my_bcomm->my_proposal = "111";
        }
        int tag_recv = -1;
        int ret = -1;
        do {
            ret = irecv(my_bcomm, &recv_buf, &tag_recv);
            if(ret == -1){
                continue;
            }

            PBuf* pbuf = malloc(sizeof(PBuf));

            pbuf_deserialize(recv_buf, pbuf);

            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            switch (tag_recv) {
                case IAR_PROPOSAL:
                    printf("Rank %d: Received proposal: %d:%s.\n", my_bcomm->my_rank, pbuf->pid, pbuf->data);
                    break;
                case IAR_VOTE:
                    printf("Rank %d: Received vote: %d:%d.\n", my_bcomm->my_rank, pbuf->pid, pbuf->vote);
                    break;
                case IAR_DECISION:
                    printf("Rank %d: Received decision: %d:%d:%s.\n", my_bcomm->my_rank, pbuf->pid, pbuf->vote, pbuf->data);
                    break;

                default:
                    printf("Warning: Rank %d: Received unexpected msg, tag = %d.\n", my_bcomm->my_rank, tag_recv);
                    break;
            }

            pbuf_free(pbuf);

        } while (tag_recv != IAR_DECISION);
    }

    MPI_Barrier(my_bcomm->my_comm);

    if(my_bcomm->my_rank == starter){
        if (result) {
            printf("\n\n =========== Proposal approved =========== \n\n");
        } else {
            printf("\n\n =========== Proposal declined =========== \n\n");
        }
    }
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
        printf("%s:%u - rank = %03d, starter_1 launching ... \n", __func__, __LINE__, my_bcomm->my_rank);
        result = iAllReduceStart(my_bcomm, my_bcomm->my_proposal, proposal_len, my_bcomm->my_rank);

    } else if(my_bcomm->my_rank  == starter_2){
        my_bcomm->IAR_active = 1;
        usleep(200); //after all started
        my_bcomm->my_proposal = "000";
        proposal_len = strlen(my_bcomm->my_proposal);
        printf("%s:%u - rank = %03d, starter_2 launching ... \n", __func__, __LINE__, my_bcomm->my_rank);
        result = iAllReduceStart(my_bcomm, my_bcomm->my_proposal, proposal_len, my_bcomm->my_rank);
    }
    else {
        usleep(500);//before starter_2
        printf("%s:%u - rank = %03d, passive ranks started \n", __func__, __LINE__, my_bcomm->my_rank);
        my_bcomm->my_proposal = "000";

        int tag_recv = -1;
        int ret = -1;
        do {

            ret = irecv(my_bcomm, &recv_buf, &tag_recv);
            if(ret == -1){
                continue;
            }

            PBuf* pbuf = malloc(sizeof(PBuf));

            pbuf_deserialize(recv_buf, pbuf);

            //printf("%s:%u - rank = %03d, passive rank received msg, tag = %d\n", __func__, __LINE__, my_bcomm->my_rank, tag_recv);
            switch (tag_recv) {
            case IAR_PROPOSAL:
                printf("Passive Rank %d: Received proposal: %d:%s.\n", my_bcomm->my_rank, pbuf->pid, pbuf->data);
                break;
            case IAR_VOTE:
                printf("Passive Rank %d: Received vote: %d:%d.\n", my_bcomm->my_rank, pbuf->pid, pbuf->vote);
                break;
            case IAR_DECISION:
                printf("Passive Rank %d: Received decision: %d:%d:%s.\n", my_bcomm->my_rank, pbuf->pid, pbuf->vote, pbuf->data);
                receved_decision++;
                break;

            default:
                printf("Warning: Passive Rank %d: Received unexpected msg, tag = %d, ret = %d.\n", my_bcomm->my_rank, tag_recv, ret);
                break;
            }

            pbuf_free(pbuf);
            usleep(600);
        } while (receved_decision < 2);
    }
    printf("Rank %d I'm done, waiting at barrier now.\n", my_bcomm->my_rank);
    MPI_Barrier(my_bcomm->my_comm);

    if(my_bcomm->my_rank == starter_1 || my_bcomm->my_rank == starter_2){
        if (result) {
            printf("\n\n =========== Proposal %s approved =========== \n\n", my_bcomm->my_proposal);
        } else {
            printf("\n\n =========== Proposal %s declined =========== \n\n", my_bcomm->my_proposal);
        }
    }
    //free(recv_buf); can' free, why?
    return -1;
}

int anycast_benchmark(bcomm* my_bcomm, int root_rank, int cnt, int buf_size){
    if(buf_size > MSG_SIZE_MAX){
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
    if(my_bcomm->my_rank == root_rank){//send
        //load data for bcast
        my_bcomm->user_send_buf = buf;
        for(int i = 0; i < cnt; i++){
            bcast(my_bcomm, BCAST);
        }

    }else{//recv
        do{
            if(irecv(my_bcomm, &recv_buf, &recved_tag) == 0){
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
    if(my_bcomm->my_rank == root_rank){
        float time_avg = time_recv/cnt;
        printf("Root: Anycast ran %d times, average costs %f usec/run\n", cnt, time_avg);
    }
    return 0;
}

int native_benchmark_single_point_bcast(MPI_Comm my_comm, int root_rank, int cnt, int buf_size){
    char* buf = calloc(buf_size, sizeof(char));
    char recv_buf[MSG_SIZE_MAX] = {'\0'};
    // native mpi bcast

    int my_rank;
    MPI_Comm_rank(my_comm, &my_rank);

    if(my_rank == root_rank){
        //sleep(1);
        unsigned long start = get_time_usec();
        MPI_Barrier(my_comm);
        for(int i = 0; i < cnt; i++){
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
            MPI_Bcast(buf, buf_size, MPI_CHAR, root_rank, my_comm);
            printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        }
        unsigned long end = get_time_usec();
        printf("Native MPI_Bcast ran %d times, average costs %lu usec/run\n", cnt, (end - start)/cnt);
    } else {
        MPI_Request req;
        MPI_Status stat;
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        MPI_Barrier(my_comm);
        for(int i = 0; i < cnt; i++){
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

int prev_rank(int my_rank, int world_size) {
    return (my_rank + (world_size - 1)) % world_size;
}

int next_rank(int my_rank, int world_size) {
    return (my_rank + 1) % world_size;
}

int random_rank(int my_rank, int world_size) {
    int next_rank;

    do {
        next_rank = rand() % world_size;
    } while(next_rank == my_rank);

    return next_rank;
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
    *(int *)next_rank = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

    /* Broadcast message */
    bcast(my_bcomm, BCAST);
    bcast_cnt = 1;

    while (bcast_cnt < cnt) {
        char *recv_buf;

        /* Retrieve a message (in internal bcomm buffer) */
        int recved_tag = 0;
        if (irecv(my_bcomm, &recv_buf, &recved_tag) == 0 && recved_tag == BCAST) {
            int recv_rank = *(int *)recv_buf;

            if (recv_rank == my_bcomm->my_rank) {
                /* If there are outstanding messages being broadcast, wait for them now, before re-using buffer */
                if(my_bcomm->bcast_send_cnt > 0) {
                    MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
                    my_bcomm->bcast_send_cnt = 0;
                } /* end if */

                /* Compose message to send (in bcomm's send buffer) */
                *(int *)next_rank = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

                /* Broadcast message */
                bcast(my_bcomm, BCAST);
                bcast_cnt++;
            }
        }
    }

    my_rank = my_bcomm->my_rank;
    recv_msg_cnt = bcomm_teardown(my_bcomm);

    time_end = get_time_usec();
    phase_1 = time_end - time_start;

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Rank %d reports:  Hacky sack done, round # = %d . received %d times.  Phase 1 cost %lu msecs\n", my_rank,
            bcast_cnt, recv_msg_cnt, phase_1 / 1000);

    return 0;
}

int main(int argc, char** argv) {
    bcomm* my_bcomm;
    int game_cnt;
    int init_rank;
    time_t t;

    //init_rank = atoi(argv[1]);
    //game_cnt = atoi(argv[2]);
    //int msg_size = atoi(argv[3]);
    //int no_rank = atoi(argv[4]);
    srand((unsigned) time(&t) + getpid());

    MPI_Init(NULL, NULL);

    //native_benchmark_single_point_bcast(MPI_COMM_WORLD, init_rank, game_cnt, msg_size);

    if(NULL == (my_bcomm = bcomm_init(MPI_COMM_WORLD, MSG_SIZE_MAX)))
        return 0;

    //anycast_benchmark(my_bcomm, init_rank, game_cnt, msg_size);

//    hacky_sack(game_cnt, init_rank, my_bcomm);
//    MPI_Barrier(my_bcomm->my_comm);

    //test_IAllReduce_single_proposal(my_bcomm, init_rank, no_rank);
    int starter_1 = atoi(argv[1]);
    int starter_2 = atoi(argv[2]);
    test_IAllReduce_multi_proposal(my_bcomm, starter_1, starter_2);
    //bcomm_teardown(my_bcomm);

    MPI_Finalize();

    return 0;
}

