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
    BCAST, SHUTDOWN
};

typedef struct BCastCommunicator {
    /* MPI fields */
    MPI_Comm my_comm;                   /* MPI communicator to use */
    int my_rank;                        /* Local rank value */
    int world_size;                     /* # of ranks in communicator */

    /* Skip ring fields */
    int my_level;                       /* Level of rank in ring */
    int last_wall;                      /* The closest rank that has higher level than rank world_size - 1 */
    int world_is_power_of_2;            /* Whether the world size is a power of 2 */

    /* Send fields */
    int send_channel_cnt;               /* # of outgoing channels from this rank */
    int* send_list;                     /* Array of outgoing ranks to send to */
    int send_list_len;
    MPI_Request* isend_reqs;            /* Array for send requests */

    /* Receive fields */
    MPI_Request irecv_req;              /* Request for incoming messages */
    char* recv_buf;                     /* Buffer for incoming messages */
    
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

//Returns the non-zero rank with highest level. This is the only rank that can send to rank 0 and rank n-1.
int tallest_rank(int world_size) {
    return 0;
}

bcomm *bcomm_init(MPI_Comm comm) {
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

    /* Skip ring counts */
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
    my_bcomm->isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));

    /* Set up receive fields */
    my_bcomm->recv_buf = (char*) malloc(MSG_SIZE_MAX * sizeof(char));
    MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &(my_bcomm->irecv_req)); //MPI_ANY_TAG

    return my_bcomm;
}

int msg_make(void* buf_inout, int origin) {
    char tmp[MSG_SIZE_MAX] = { 0 };

    memcpy(tmp, &origin, sizeof(int));
    memcpy(tmp + sizeof(int), buf_inout, MSG_SIZE_MAX - sizeof(int));
    memcpy(buf_inout, &tmp, MSG_SIZE_MAX);
    return 0;
}

int msg_get_num(void* buf_in) {
    return *((int*) buf_in);
}

int msg_life_update(void* buf_inout) { //return life before change
    //printf("before update: %s, ", buf_inout);
    int life_left;
    char tmp;
    if (strlen(buf_inout) <= 0) {
        return -1;
    }
    memcpy(&tmp, buf_inout, 1);
    //assume life <= 9
    life_left = tmp - '0';
    if (life_left == 0) {
        return 0;
    }
    tmp = (life_left - 1) + '0';
    memcpy(buf_inout, &tmp, 1);

    return life_left;
}

// Event progress tracking
int check_passed_origin(bcomm* my_bcomm, int origin_rank, int to_rank) {
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
int recv_forward(bcomm* my_bcomm, char* recv_buf_out) {
    MPI_Status status;
    int completed = 0;

    /* Check if we've received any messages */
    MPI_Test(&my_bcomm->irecv_req, &completed, &status);
    if (completed) {
        /* Increment # of messages received */
        my_bcomm->bcast_recv_cnt++;

        /* Copy received message */
        memcpy(recv_buf_out, my_bcomm->recv_buf + sizeof(int), MSG_SIZE_MAX - sizeof(int));

        /* Check for a rank that can forward messages */
        if(my_bcomm->my_level > 0) {
            int origin;
            int send_cnt;

            /* Retrieve message's origin rank */
            origin = msg_get_num(my_bcomm->recv_buf);

            /* Determine which ranks to send to */
            send_cnt = 0;
            if (status.MPI_SOURCE > my_bcomm->last_wall) {
                /* Send messages, to further ranks first */
                for (int j = my_bcomm->send_channel_cnt; j >= 0; j--) {
                    MPI_Isend(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], BCAST, my_bcomm->my_comm,
                            &(my_bcomm->isend_reqs[send_cnt]));
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
                            MPI_Isend(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], BCAST, my_bcomm->my_comm,
                                    &(my_bcomm->isend_reqs[send_cnt]));
                            send_cnt++;
                        }
                    }
                } /* end if */
            } /* end else */

            /* Wait for all messages to be sent */
            if(send_cnt > 0) {
                MPI_Status isend_stat[send_cnt];

                MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);
            } /* end if */
        } /* end if */

        /* Re-post receive, for next message */
        MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm,
                &my_bcomm->irecv_req);

        return 0;
    }

    return -1;
}

// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_bcomm, void* send_buf, int send_size) {
    MPI_Status isend_stat[my_bcomm->send_list_len];

    msg_make(send_buf, my_bcomm->my_rank);

    /* Send to all receivers, further away first */
    for (int i = my_bcomm->send_list_len - 1; i >= 0; i--)
        MPI_Isend(send_buf, send_size, MPI_CHAR, my_bcomm->send_list[i], BCAST, my_bcomm->my_comm,
                &my_bcomm->isend_reqs[i]);
    MPI_Waitall(my_bcomm->send_list_len, my_bcomm->isend_reqs, isend_stat);

    my_bcomm->my_bcast_cnt++;

    return 0;
}

int bcomm_teardown(bcomm* my_bcomm) {
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat;
    int done;
    char recv_buf[MSG_SIZE_MAX];
    int recv_cnt;

    /* Retrieve the # of broadcasts from all ranks */
    MPI_Iallreduce(&(my_bcomm->my_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, my_bcomm->my_comm, &req);

    /* Forward messages until all ranks have participated in allreduce for total braoadcast count */
    do {
        MPI_Test(&req, &done, &stat);
        if (!done)
            recv_forward(my_bcomm, recv_buf);
    } while (!done);

    /* Forward messages until we've received all the broadcasts */
    while (my_bcomm->bcast_recv_cnt + my_bcomm->my_bcast_cnt < total_bcast)
        recv_forward(my_bcomm, recv_buf);

    /* Retrieve the # of broadcasts we've received */
    recv_cnt = my_bcomm->bcast_recv_cnt;

    /* Shut down skip ring */
    MPI_Cancel(&my_bcomm->irecv_req);
    MPI_Comm_free(&my_bcomm->my_comm);
    free(my_bcomm->isend_reqs);
    free(my_bcomm->recv_buf);
    free(my_bcomm->send_list);
    free(my_bcomm);

    return recv_cnt;
}

int prev_rank(int my_rank, int world_size) {
    return (my_rank + (world_size - 1)) % world_size;
}

int random_rank(int my_rank, int world_size) {
    int next_rank;

    do {
        next_rank = rand() % world_size;
    } while(next_rank == my_rank);

    return next_rank;
}

int hacky_sack(int cnt, int starter, bcomm* my_bcomm) {
    char recv_buf[MSG_SIZE_MAX] = { '\0' };
    char next_rank_str[MSG_SIZE_MAX];
    unsigned long time_start;
    unsigned long time_end;
    unsigned long phase_1;
    int bcast_cnt;
    int recv_msg_cnt;
    int my_rank;

    *(int *)next_rank_str = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

    time_start = get_time_usec();

    bcast(my_bcomm, next_rank_str, MSG_SIZE_MAX);
    bcast_cnt = 1;

    while (bcast_cnt < cnt) {
        if (recv_forward(my_bcomm, recv_buf) == 0) {
            int recv_rank = *(int *)recv_buf;

            if (recv_rank == my_bcomm->my_rank) {
                *(int *)next_rank_str = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);

                bcast(my_bcomm, next_rank_str, MSG_SIZE_MAX);
                bcast_cnt++;
            }
        }
    }

    time_end = get_time_usec();
    phase_1 = time_end - time_start;

    my_rank = my_bcomm->my_rank;
    recv_msg_cnt = bcomm_teardown(my_bcomm);

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Rank %d reports:  Hacky sack done, round # = %d . received %d times.  Phase 1 cost %lu msecs\n", my_rank,
            bcast_cnt, recv_msg_cnt, phase_1 / 1000);

    return 0;
}

int main(int argc, char** argv) {
    bcomm* my_comm;
    int game_cnt;
    int init_rank;
    time_t t;

    srand((unsigned) time(&t) + getpid());

    MPI_Init(NULL, NULL);

    if(NULL == (my_comm = bcomm_init(MPI_COMM_WORLD)))
        return 0;

    game_cnt = atoi(argv[2]);
    init_rank = atoi(argv[1]);

    hacky_sack(game_cnt, init_rank, my_comm);

    MPI_Finalize();

    return 0;
}

