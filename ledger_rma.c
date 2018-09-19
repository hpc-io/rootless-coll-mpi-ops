#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <sys/time.h>

#include <pthread.h>

#define MSG_SIZE_MAX 512
enum COM_TAGS {
    BCAST,
    IAR_PROPOSAL,
    IAR_VOTE, /* vote for I_All_Reduce */
    IAR_DECISION /* Final result */
};

typedef int SN;
typedef int Vote;


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
    char* IAR_recv_buf;                 /* IallReduce recv buf */
    Vote my_vote;                       /* 1 for agree, 0 for decline */
    char* my_proposal;                  /* This is used to compare against received proposal, shuold be updated timely */
    char* send_buf_my_vote;
    int recv_vote_cnt;
    int recv_proposal_from;             /* The rank from where I received a proposal, also report votes to this rank. */
    int proposal_sent_cnt;              /* How many children received this proposal, it's the number of votes expected. */

    /* Operation counters */
    int my_bcast_cnt;
    int bcast_recv_cnt;
} bcomm;

char DEBUG_MODE = 'O';
typedef struct {
    FILE* log_file;
    int my_rank;
} Log;
Log MY_LOG;

//IAR msg formats
// | SN sn | char* proposal_content |
typedef struct Proposal_buf{
    SN sn;
    unsigned int data_len;
    char* data;
}PBuf;

int pbuf_serialize(SN sn_in, unsigned int data_len_in, char* data_in, char* buf_out, unsigned int* buf_len_out){
    if(!data_in || !buf_out || data_len_in < 1)
        return -1;
    memcpy(buf_out, &sn_in, sizeof(SN));
    memcpy(buf_out + sizeof(SN), &data_len_in, sizeof(unsigned int));
    memcpy(buf_out + sizeof(SN) + sizeof(unsigned int), data_in, data_len_in);
    if(buf_len_out){
        *buf_len_out = sizeof(SN)  /* SN */
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
    memcpy(&(pbuf_out->sn), buf_in, sizeof(SN));

    memcpy(&(pbuf_out->data_len), buf_in + sizeof(SN), sizeof(unsigned int));
    //printf("%s: sn = %d, data_len = %u\n", __func__, pbuf_out->sn, pbuf_out->data_len);
    //printf("%s:%u \n", __func__, __LINE__);
    pbuf_out->data = malloc(MSG_SIZE_MAX - sizeof(SN));
    //printf("%s:%u \n", __func__, __LINE__);
    memcpy(pbuf_out->data, buf_in + sizeof(SN) + sizeof(unsigned int), pbuf_out->data_len);
    return 0;
}

int pbuf_deserialize_t(char* buf_in, SN* sn_out, unsigned int* data_len_out, char* data_out){
    if(!buf_in)
        return -1;

    if(sn_out)
        memcpy(sn_out, buf_in, sizeof(SN));

    unsigned int len = 0;
    memcpy(&len, buf_in + sizeof(SN), sizeof(unsigned int));

    if(data_len_out)
        *data_len_out = len;

    if(data_out)
        memcpy(data_out, buf_in + sizeof(SN) + sizeof(unsigned int), len);
    return 0;
}

typedef struct Vote_buf{
    SN sn;
    Vote vote;
}VBuf;

SN make_sn(bcomm* my_bcomm){
    return (SN) my_bcomm->my_rank;
}

int agree_proposal(char* prop_1, char* prop_2);

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
    my_bcomm->IAR_recv_buf = (char*)malloc(sizeof(int) + sizeof(SN) + msg_size_max);                 /* IallReduce recv buf */
    my_bcomm->my_vote = 1;                       /* 1 for agree, 0 for decline, default is 1 */
    my_bcomm->my_proposal = (char*)malloc(msg_size_max);                  /* This is used to compare against received proposal, shuold be updated timely */
    my_bcomm->send_buf_my_vote = (char*)malloc(sizeof(int) + sizeof(SN) + sizeof(Vote));
    my_bcomm->recv_vote_cnt = 0;
    my_bcomm->recv_proposal_from = -1;
    my_bcomm->proposal_sent_cnt = -1;

    /* Post initial receive for this rank */
    MPI_Irecv(my_bcomm->recv_buf[0], msg_size_max + sizeof(int), MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, my_bcomm->my_comm, &(my_bcomm->irecv_req));

    return my_bcomm;
}

int msg_get_num(void* buf_in) {
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

int recv_forward(bcomm* my_bcomm, char** recv_buf_out, int* recved_tag_out) {
    MPI_Status status;
    int completed = 0;

    /* Check if we've received any messages */
    MPI_Test(&my_bcomm->irecv_req, &completed, &status);
    if (completed) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        if(recved_tag_out)
            *recved_tag_out = status.MPI_TAG;

        if(status.MPI_TAG == IAR_VOTE){//collect vote and up stream
            if(recv_buf_out)// not NULL
                *recv_buf_out = (char *) my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];
            //printf("%s:%u - rank = %03d, received a vote.\n", __func__, __LINE__, my_bcomm->my_rank);
            my_bcomm->recv_vote_cnt ++;
            //printf("%s:%u - rank = %03d, received vote from rank %d, recv_vote_cnt = %d, expected total is %d\n", __func__, __LINE__, my_bcomm->my_rank, status.MPI_SOURCE, my_bcomm->recv_vote_cnt, my_bcomm->send_list_len);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            PBuf* vote_buf = malloc(sizeof(PBuf));
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

            pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index], vote_buf);// + sizeof(int). IAR_VOTE doesn't need origin
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            //my_bcomm->my_vote &= *(Vote*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int) + sizeof(SN));//get received vote
            my_bcomm->my_vote &= *(Vote*)(vote_buf->data);//*(Vote*)(vote_buf->data)
            //printf("%s:%u - rank %03d received a vote: %d, now my vote = %d\n", __func__, __LINE__, my_bcomm->my_rank, *(Vote*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(SN)+ sizeof(unsigned int)), my_bcomm->my_vote);
            if(my_bcomm->recv_vote_cnt == my_bcomm->proposal_sent_cnt){//all votes are received, report to predecessor
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
                //memcpy(my_bcomm->send_buf_my_vote, my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), sizeof(SN));
                //*(my_bcomm->send_buf_my_vote + sizeof(SN)) = my_bcomm->my_vote; // vote NO

                *(Vote*) (vote_buf->data) = my_bcomm->my_vote;
                unsigned int send_len;
                pbuf_serialize(vote_buf->sn, sizeof(Vote), vote_buf->data, my_bcomm->send_buf_my_vote, &send_len);
                MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)

                bufer_maintain_irecv(my_bcomm);
                pbuf_free(vote_buf);

                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
                return 3;//received all votes
            }

            bufer_maintain_irecv(my_bcomm);
            pbuf_free(vote_buf);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            return 2;//received a vote, but expecting more
        }

        if(status.MPI_TAG == IAR_PROPOSAL){// new proposal, down stream
            if(recv_buf_out)// not NULL
                *recv_buf_out = (char *)my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index]+ sizeof(int);

            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            my_bcomm->recv_proposal_from = status.MPI_SOURCE;
            my_bcomm->proposal_sent_cnt = 0;

            PBuf* pbuf = malloc(sizeof(PBuf));
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            pbuf_deserialize(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), pbuf);
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            if (0 == agree_proposal(my_bcomm->my_proposal, pbuf->data)) {//local declined, up stream to predecessor
                //printf("recv_forward: received proposal: sn = %d, proposal: %s\n", pbuf->sn, pbuf->data);
                //printf("%s:%u - rank = %03d, proposal declined, reporting...\n", __func__, __LINE__, my_bcomm->my_rank);
                //copy sn from send[0]
                //memcpy(my_bcomm->send_buf_my_vote, my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), sizeof(SN));//copy sn
                //set vote
                my_bcomm->my_vote = 0;
                unsigned int send_len;
                pbuf_serialize(pbuf->sn, sizeof(Vote), (char*)&(my_bcomm->my_vote), my_bcomm->send_buf_my_vote, &send_len);

                //*(my_bcomm->send_buf_my_vote + sizeof(SN)) = my_bcomm->my_vote; // vote NO
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
                MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);// sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);

                bufer_maintain_irecv(my_bcomm);
                pbuf_free(pbuf);
                return 1;//proposal declined locally, reported
            }else{//local approved
                if(my_bcomm->send_channel_cnt == 0){//leaf rank vote yes
                    memcpy(my_bcomm->send_buf_my_vote, my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int), sizeof(SN));//copy sn

                    my_bcomm->my_vote = 1;
                    unsigned int send_len;
                    pbuf_serialize(*(SN*)(my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index] + sizeof(int)), sizeof(Vote), (char*)&(my_bcomm->my_vote), my_bcomm->send_buf_my_vote, &send_len);
                    //*(Vote*)(my_bcomm->send_buf_my_vote + sizeof(SN)) = my_bcomm->my_vote; // vote NO
                    MPI_Send(my_bcomm->send_buf_my_vote, send_len, MPI_CHAR, my_bcomm->recv_proposal_from, IAR_VOTE, my_bcomm->my_comm);//sizeof(SN) + sizeof(unsigned int) + sizeof(Vote)

                    bufer_maintain_irecv(my_bcomm);
                    pbuf_free(pbuf);
                    return 4;//leaf rank finished.
                }
            }
            // else case: regular forward
        }
        //BCAST and DECISION tags are treated the same.
            void *recv_buf;

            /* Increment # of messages received */
            my_bcomm->bcast_recv_cnt++;

            /* Set buffer that message was received in */
            recv_buf = my_bcomm->recv_buf[my_bcomm->curr_recv_buf_index];

            /* Check for a rank that can forward messages */
            if(my_bcomm->my_level > 0) {
                int origin;
                int send_cnt;

                /* Retrieve message's origin rank */
                origin = msg_get_num(recv_buf);

                /* Determine which ranks to send to */
                send_cnt = 0;
                if (status.MPI_SOURCE > my_bcomm->last_wall) {
                    /* Send messages, to further ranks first */
                    for (int j = my_bcomm->send_channel_cnt; j >= 0; j--) {
                        MPI_Isend(recv_buf, my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, my_bcomm->send_list[j],  status.MPI_TAG, my_bcomm->my_comm,
                                &(my_bcomm->fwd_isend_reqs[my_bcomm->curr_recv_buf_index][send_cnt]));
                        send_cnt++;
                    }
                } /* end if */
                else {
                    int upper_bound;

                    upper_bound = my_bcomm->send_channel_cnt - 1; // not send to same level

                    /* Avoid situation where world_size - 1 rank in non-power of 2 world_size shouldn't forward */
                    if(upper_bound >= 0) {
                        /* Send messages, to further ranks first */
                        for (int j = upper_bound; j >= 0; j--) {
                            if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
                                MPI_Isend(recv_buf, my_bcomm->msg_size_max + sizeof(int), MPI_CHAR, my_bcomm->send_list[j],  status.MPI_TAG, my_bcomm->my_comm,
                                        &(my_bcomm->fwd_isend_reqs[my_bcomm->curr_recv_buf_index][send_cnt]));
                                send_cnt++;
                            }
                        }
                    } /* end if */
                } /* end else */

                /* Update # of outstanding messages being sent for bcomm */
                my_bcomm->fwd_send_cnt[my_bcomm->curr_recv_buf_index] = send_cnt;
                if(my_bcomm->proposal_sent_cnt == 0)
                    my_bcomm->proposal_sent_cnt = send_cnt;
            } /* end if */

            /* Return pointer to user data in current receive buffer */
            if(recv_buf_out)// not NULL
                *recv_buf_out = ((char *)recv_buf) + sizeof(int);

            bufer_maintain_irecv(my_bcomm);
            return 0;
    }

    return -1;
}

// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_bcomm, enum COM_TAGS tag) {
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    /* If there are outstanding messages being broadcast, wait for them now */
    if(my_bcomm->bcast_send_cnt > 0) {
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        MPI_Waitall(my_bcomm->bcast_send_cnt, my_bcomm->bcast_isend_reqs, my_bcomm->bcast_isend_stats);
        my_bcomm->bcast_send_cnt = 0;
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    } /* end if */
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--){
        MPI_Isend(my_bcomm->send_buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm,
                &my_bcomm->bcast_isend_reqs[i]);// my_bcomm->my_comm
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        //MPI_Send(my_bcomm->send_buf, my_bcomm->msg_size_max, MPI_CHAR, my_bcomm->send_list[i], tag, my_bcomm->my_comm);
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    }
    /* Update # of outstanding messages being sent for bcomm */
    my_bcomm->bcast_send_cnt = my_bcomm->send_list_len;
    my_bcomm->my_bcast_cnt++;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
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
        if (!done)
            recv_forward(my_bcomm, &recv_buf, NULL);
    } while (!done);

    /* Forward messages until we've received all the broadcasts */
    while (my_bcomm->bcast_recv_cnt + my_bcomm->my_bcast_cnt < total_bcast)
        recv_forward(my_bcomm, &recv_buf, NULL);

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

// A toy comparitor
int agree_proposal(char* prop_1, char* prop_2){
    int ret = strcmp(prop_1, prop_2);
    if(ret == 0)
        return 1;
    else
        return 0;
    //    if(prop_1[0] == prop_2[0])
//        return 1;
//    else
//        return 0;
}



int iAllReduceStart(bcomm* my_bcomm, char* proposal, unsigned long prop_size){
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    char* recv_buf = malloc(2* MSG_SIZE_MAX);
    SN sn = 1234;//make_sn(my_bcomm);
    unsigned buf_len;
    if(0 != pbuf_serialize(sn, prop_size, proposal, my_bcomm->user_send_buf, &buf_len)){
        printf("pbuf_serialize failed.\n");
        return -1;
    }

    bcast(my_bcomm, IAR_PROPOSAL);//IAR_PROPOSAL

    int recv_vote_cnt = 0;
    int recved_tag = 0;
    Vote votes_result = 1;
    Vote recv_vote = 1;
    int ret = -1;
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    while (recv_vote_cnt < my_bcomm->send_list_len) { //use send_list_len ONLY on the started, other places use proposal_sent_cnt.

        ret = recv_forward(my_bcomm, &recv_buf, &recved_tag);

        if(ret == 3 || ret == 2 || ret == 1){//received all votes
            recv_vote_cnt++;
            votes_result &= my_bcomm->my_vote;
            printf("%s:%u - rank = %03d, received vote, recv_vote_cnt = %d, waiting for %d\n", __func__, __LINE__, my_bcomm->my_rank, recv_vote_cnt, my_bcomm->send_channel_cnt);
        }
        if ( ret== 0) {
            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            if (recved_tag == IAR_PROPOSAL) {
                // TODO: multi proposal cases
            }
            if (recved_tag == IAR_VOTE) {

            }
        }
    }
    printf("%s:%u - rank = %03d, bcasting final decision\n", __func__, __LINE__, my_bcomm->my_rank);
    memcpy(my_bcomm->user_send_buf + sizeof(SN) + sizeof(unsigned int), &votes_result, sizeof(Vote));
    bcast(my_bcomm, IAR_DECISION);
    //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
    free(recv_buf);
    return votes_result;
}

int test_IAllReduce(bcomm* my_bcomm, int starter) {
    char* my_proposal = "111";
    char* recv_buf = malloc(2 * MSG_SIZE_MAX);
    //printf("Rank %d: send_channel_cnt = %d, send_list_len = %d\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, my_bcomm->send_list_len);
    if (my_bcomm->my_rank == starter) {
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        int len = strlen(my_proposal);
        int result = iAllReduceStart(my_bcomm, my_proposal, len);
        if (result) {
            printf("Proposal approved\n");
        } else {
            printf("Proposal declined\n");
        }
    } else {
        usleep(1500);
        if (my_bcomm->my_rank % 2 == 0) {
            my_bcomm->my_proposal = "111";
        } else {
            my_bcomm->my_proposal = "000";
        }
        int tag_recv = -1;
        int ret = -1;
        do {
            ret = recv_forward(my_bcomm, &recv_buf, &tag_recv);
            if(ret == -1){
                continue;
            }
//            if(ret == 3 || ret == 2 || ret == 1){//received all votes
//                recv_vote_cnt++;
//                votes_result &= my_bcomm->my_vote;
//            }

            PBuf* pbuf = malloc(sizeof(PBuf));

            pbuf_deserialize(recv_buf, pbuf);

            //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
            switch (tag_recv) {
            case IAR_PROPOSAL:
                printf("Rank %d: Received proposal: %d:%s\n", my_bcomm->my_rank, pbuf->sn, pbuf->data);
                break;
            case IAR_VOTE:
                printf("Rank %d: Received vote: %d:%d\n", my_bcomm->my_rank, pbuf->sn, *(Vote*)(pbuf->data));
                break;
            case IAR_DECISION:
                printf("Rank %d: Received decision: %d:%d\n", my_bcomm->my_rank, pbuf->sn, *(Vote*)(pbuf->data));
                break;

            default:
                printf("Warning: Rank %d: Received unexpected msg!!\n", my_bcomm->my_rank);
                break;
            }

            pbuf_free(pbuf);

        } while (tag_recv != IAR_DECISION);
    }
    //free(recv_buf); can' free, why?
    return -1;
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
        if (recv_forward(my_bcomm, &recv_buf, &recved_tag) == 0 && recved_tag == BCAST) {
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

    srand((unsigned) time(&t) + getpid());

    MPI_Init(NULL, NULL);

    if(NULL == (my_bcomm = bcomm_init(MPI_COMM_WORLD, MSG_SIZE_MAX)))
        return 0;

    //game_cnt = atoi(argv[2]);
    init_rank = atoi(argv[1]);

//    hacky_sack(game_cnt, init_rank, my_bcomm);
//    MPI_Barrier(my_bcomm->my_comm);
    test_IAllReduce(my_bcomm, init_rank);
    //int a = 1;
    //int b = 0;
    //printf("a&b = %d, a|b = %d\n", a&b, a|b);
    MPI_Finalize();

    return 0;
}

