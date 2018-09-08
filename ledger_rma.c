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
    MPI_Comm my_comm;
    int my_rank;
    int send_channel_cnt; //my_level
    int world_size;
    int msg_life_max;
    int is_bidirectional;
    //int* send_list_left;
    int* send_list;
    //int send_list_left_len;
    int send_list_len;
    int* recv_list;
    //int* recv_list_right;
    int recv_list_len;
    //int recv_list_right_len;

    int last_recv_it;
    MPI_Request irecv_req;
    MPI_Request* isend_reqs;
    char* recv_buf;
    int my_bcast_cnt;
    int bcast_recv_cnt;
//char** recv_buf;//2D array

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
    if (is_powerof2(world_size)) {
        if (rank == 0) {
            return log2(world_size) - 1;
        }
    } else {
        if (rank == 0) {
            return log2(world_size);
        }
// DO NOT set for world_size - 1!
//        if(rank == world_size - 1)
//            return 0;
    }

    int l = 0;
    while (rank != 0 && rank % 2 == 0) {
        rank >>= 1;
        l++;
    }
    return l;
}

//This returns the closest rank that has higher rank than rank world_size - 1
int last_wall(int world_size) {
    int last_level = get_level(world_size, world_size - 1);
    for (int i = world_size - 2; i > 0; i--) {
        if (get_level(world_size, i) > last_level) {
            return i;
        }
    }
    return 0; //not found
}

//Returns the non-zero rank with highest level. This is the only rank that can send to rank 0 and rank n-1.
int tallest_rank(int world_size) {
    return 0;
}

int bcomm_init(bcomm* my_bcomm, MPI_Comm comm) {
    my_bcomm->my_comm = comm;
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    if (my_bcomm->world_size < 2) {
        printf("Too few ranks, program ended. world_size = %d\n", my_bcomm->world_size);
        return -1;
    }
    my_bcomm->my_bcast_cnt = 0;
    my_bcomm->bcast_recv_cnt = 0;
    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);
    my_bcomm->send_channel_cnt = get_level(my_bcomm->world_size, my_bcomm->my_rank);

    my_bcomm->msg_life_max = (int) log2(my_bcomm->world_size) + 1;

    int is_power2 = is_powerof2(my_bcomm->world_size);

    if (is_power2) { //OK
        my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;
        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for (int i = 0; i < my_bcomm->send_list_len; i++) {
            my_bcomm->send_list[i] = (int) (my_bcomm->my_rank + pow(2, i)) % my_bcomm->world_size;
        }
    } else { // non 2^n world size
        my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;

        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for (int i = 0; i <= my_bcomm->send_channel_cnt; i++) {
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

                my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;
            } /* end if */
            else {
                my_bcomm->send_list[i] = send_dest;
            }

        }
    }

    my_bcomm->recv_buf = (char*) malloc(MSG_SIZE_MAX * sizeof(char));
    my_bcomm->isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));

    MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &(my_bcomm->irecv_req)); //MPI_ANY_TAG

    return 0;
}

int msg_make(void* buf_inout, int origin, int sn) {
    char tmp[MSG_SIZE_MAX] = { 0 };
    memcpy(tmp, &origin, sizeof(int));
    memcpy(tmp + sizeof(int), &sn, sizeof(int));
    memcpy(tmp + 2 * sizeof(int), buf_inout, MSG_SIZE_MAX - 2 * sizeof(int));
    memcpy(buf_inout, &tmp, MSG_SIZE_MAX);
    return 0;
}

int msg_get_num(void* buf_in) {
    //return ((int*) buf_in)[0];
    int r = 0;
    memcpy(&r, buf_in, sizeof(int));
    return r;
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
    if (to_rank == origin_rank) {
        return 1;
    }

    int my_rank = my_bcomm->my_rank;

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
    MPI_Test(&my_bcomm->irecv_req, &completed, &status);

    if (completed) {
        my_bcomm->bcast_recv_cnt++;

        int origin = msg_get_num(my_bcomm->recv_buf);

        memcpy(recv_buf_out, my_bcomm->recv_buf + sizeof(int), MSG_SIZE_MAX - sizeof(int));

        int recv_level = get_level(my_bcomm->world_size, status.MPI_SOURCE);

        int send_cnt = 0;
        int upper_bound = 0;
        int tmp_my_level = get_level(my_bcomm->world_size, my_bcomm->my_rank);

        if (recv_level <= tmp_my_level) { // my_bcomm->my_level; new msg, send to all channels//|| my_bcomm->my_rank != my_bcomm->world_size - 1
            upper_bound = my_bcomm->send_channel_cnt;
        } else {
            upper_bound = my_bcomm->send_channel_cnt - 1; // not send to same level
        }

        int lw = last_wall(my_bcomm->world_size);
        if (my_bcomm->my_rank == my_bcomm->world_size - 1 && !is_powerof2(my_bcomm->world_size)
                && status.MPI_SOURCE != lw) {
            upper_bound = my_bcomm->send_channel_cnt; //forward all
        }

        for (int j = 0; j <= upper_bound; j++) {
            if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
                send_cnt++;
                MPI_Isend(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], BCAST, my_bcomm->my_comm,
                        &(my_bcomm->isend_reqs[j]));
            }
        }

        MPI_Status isend_stat[send_cnt];
        MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);
        MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm,
                &my_bcomm->irecv_req);
        return 0;
    }
    return -1;
}

// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_bcomm, void* send_buf, int sn, int send_size) {
    msg_make(send_buf, my_bcomm->my_rank, sn);

    for (int i = 0; i < my_bcomm->send_list_len; i++) {
        MPI_Isend(send_buf, send_size, MPI_CHAR, my_bcomm->send_list[i], BCAST, MPI_COMM_WORLD,
                &my_bcomm->isend_reqs[i]);
    }

    my_bcomm->my_bcast_cnt++;
    MPI_Status isend_stat[my_bcomm->send_list_len];
    MPI_Waitall(my_bcomm->send_list_len, my_bcomm->isend_reqs, isend_stat);
    return 0;
}

char result_buf[2 * 1024] = { '\0' };

int bcomm_teardown(bcomm* my_bcomm) {
    int total_bcast = 0;
    MPI_Request req;
    MPI_Status stat;
    MPI_Iallreduce(&(my_bcomm->my_bcast_cnt), &total_bcast, 1, MPI_INT, MPI_SUM, my_bcomm->my_comm, &req);

    int done = 0;
    char recv_buf[MSG_SIZE_MAX];
    do {
        MPI_Test(&req, &done, &stat);
        if (!done) {
            if (recv_forward(my_bcomm, recv_buf) == 0) {
//                int recv_rank = atoi(recv_buf + sizeof(int));
//                char origin_rank[16] = {'\0'};
//                sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
//                strcat(result_buf, origin_rank);//ranks
//                strcat(result_buf, ",");
            }
        }
    } while (!done);

    while (my_bcomm->bcast_recv_cnt + my_bcomm->my_bcast_cnt < total_bcast) {
        if (recv_forward(my_bcomm, recv_buf) == 0) {
//            int recv_rank = atoi(recv_buf + sizeof(int));
//            char origin_rank[16] = {'\0'};
//            sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
//            strcat(result_buf, origin_rank);//ranks
//            strcat(result_buf, ",");
        }
    }

    int recv_cnt = my_bcomm->bcast_recv_cnt;
    MPI_Cancel(&my_bcomm->irecv_req);

    free(my_bcomm);
    return recv_cnt;
}

int prev_rank(int my_rank, int world_size) {
    return (my_rank - 1) % world_size;
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
    int next_rank;
    char next_rank_str[MSG_SIZE_MAX];
    int sn = 0;
    unsigned long time_start;
    unsigned long time_end;
    unsigned long phase_1;
    int bcast_cnt;
    int recv_msg_cnt;
    int my_rank;

    next_rank = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);
    snprintf(next_rank_str, 10, "%d", next_rank);

    time_start = get_time_usec();

    bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
    bcast_cnt = 1;

    while (bcast_cnt < cnt) {
        if (recv_forward(my_bcomm, recv_buf) == 0) {
            int recv_rank = atoi(recv_buf + sizeof(int));

            if (recv_rank == my_bcomm->my_rank) {
                int next_rank;

                bcast_cnt++;
                next_rank = prev_rank(my_bcomm->my_rank, my_bcomm->world_size);
                snprintf(next_rank_str, 10, "%d", next_rank);
                sn++;
                bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
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

    my_comm = malloc(sizeof(bcomm));
    if(bcomm_init(my_comm, MPI_COMM_WORLD) != 0)
        return 0;

    game_cnt = atoi(argv[2]);
    init_rank = atoi(argv[1]);

    hacky_sack(game_cnt, init_rank, my_comm);

    MPI_Finalize();

    return 0;
}

