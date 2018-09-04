#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <time.h>

#include <pthread.h>


#define MSG_SIZE_MAX 512

int TMP_SWITCH = 0;
typedef struct BCastCommunicator {
    MPI_Comm my_comm;
    int my_rank;
    int send_channel_cnt;//my_level
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
    //char** recv_buf;//2D array

} bcomm;

char DEBUG_MODE = 'O';
typedef struct{
    FILE* log_file;
    int my_rank;
}
Log;
Log MY_LOG;

void get_time_str(char *str_out){
    time_t rawtime;
    struct tm * timeinfo;
    time ( &rawtime );
    timeinfo = localtime ( &rawtime );
    sprintf(str_out, "%d:%d:%d", timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}

void log_init(MPI_Comm my_comm, Log my_log){
    char fname[128];
    MPI_Comm_rank(my_comm, &(my_log.my_rank));
    char time_str[64];
    get_time_str(time_str);
    sprintf(fname, "Log_rank_%03d_%s.log", my_log.my_rank, time_str);
    my_log.log_file = fopen(fname, "w+");
}

void log_close(Log my_log){
    if(DEBUG_MODE == 'F'){
        fflush(my_log.log_file);
        fclose(my_log.log_file);
    }
    return;
}

void debug(Log my_log){
    if(DEBUG_MODE == 'O'){
        return;
    }

    char log_line[MSG_SIZE_MAX];
    sprintf(log_line, "%s:%u - rank = %03d\n", __func__, __LINE__, my_log.my_rank);
    switch(DEBUG_MODE){
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

int is_powerof2(int n){
    while(n != 1 && n % 2 == 0){
        n >>= 1;
    }
    if(n == 1){
        return 1;
    } else {
        return 0;
    }
}

int get_level(int world_size, int rank){
    if(is_powerof2(world_size)){
        if(rank == 0){
            return log2(world_size) - 1;
        }
    }else{
        if(rank == 0){
            return log2(world_size);
        }
// DO NOT set for world_size - 1!
//        if(rank == world_size - 1)
//            return 0;
    }

    int l = 0;
    while(rank != 0 && rank % 2 == 0){
        rank >>= 1;
        l++;
    }
    return l;
}

//This returns the closest rank that has higher rank than rank world_size - 1
int last_wall(int world_size){
    int last_level = get_level(world_size, world_size - 1);
    for(int i = world_size - 2; i > 0; i--){
        if(get_level(world_size, i) > last_level){
            return i;
        }
    }
    return 0;//not found
}

//Returns the non-zero rank with highest level. This is the only rank that can send to rank 0 and rank n-1.
int tallest_rank(int world_size){
    return 0;
}

int routing_table_init(bcomm* my_bcomm, MPI_Comm comm) {
    my_bcomm->my_comm = comm;
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    if(my_bcomm->world_size < 2){
        printf("Too few ranks, program ended. world_size = %d\n", my_bcomm->world_size);
        return -1;
    }

    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);
    my_bcomm->send_channel_cnt = get_level(my_bcomm->world_size, my_bcomm->my_rank);
    //printf("rank = %d, level = %d\n", my_bcomm->my_rank, my_bcomm->my_level);
    my_bcomm->msg_life_max = (int) log2(my_bcomm->world_size) + 1; //(int) log2(my_bcomm->world_size);

    int is_power2 = is_powerof2(my_bcomm->world_size);

    if(is_power2){//OK
        my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;
        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for(int i = 0; i <= my_bcomm->send_channel_cnt; i++){
            my_bcomm->send_list[i] = (int)(my_bcomm->my_rank + pow(2, i)) %  my_bcomm->world_size;
            //printf("rank = %d, send_list[%d] = %d\n", my_bcomm->my_rank, i, my_bcomm->send_list[i]);
            //printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i]);
        }
    }else{ // non 2^n world size
        my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;

        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for(int i = 0; i <= my_bcomm->send_channel_cnt; i++){
            int send_dest = (int)(my_bcomm->my_rank + pow(2, i));

            /* Check for sending to ranks beyond the end of the world size */
            if(send_dest >= my_bcomm->world_size) {
                if(my_bcomm->my_rank == (my_bcomm->world_size - 1)) {
                    my_bcomm->send_channel_cnt = 0;
                    my_bcomm->send_list[0] = 0;
                    //printf("rank = %d, send_list[%d] = %d\n", my_bcomm->my_rank, 0, my_bcomm->send_list[0]);
                } /* end if */
                else {
                    if(my_bcomm->send_channel_cnt != i){
                        //printf("Warning!! Rank %d level is changed from %d to %d, dest set to 0. \n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, i);
                    }
                    my_bcomm->send_channel_cnt = i;
                    my_bcomm->send_list[i] = 0;
                    //printf("rank = %d, send_list[%d] = %d\n", my_bcomm->my_rank, i, my_bcomm->send_list[i]);
                } /* end else */

                my_bcomm->send_list_len = my_bcomm->send_channel_cnt + 1;
            } /* end if */
            else {
                my_bcomm->send_list[i] = send_dest;
                //printf("rank = %d, send_list[%d] = %d\n", my_bcomm->my_rank, i, my_bcomm->send_list[i]);
            }

            //if(my_bcomm->my_rank != my_bcomm->world_size - 1){
                //printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, my_bcomm->send_list[i]);
            //}
        }
#ifdef QAK
        if(my_bcomm->my_rank == my_bcomm->world_size - 1){//patch for rank n-1
            my_bcomm->send_list[0] = 0;
            //printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[0]);
            for(int i = 1; i < my_bcomm->send_channel_cnt; i++){
                my_bcomm->send_list[i] = -1;
                //printf("Rank %d, level = %d send_to %dn", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i]);
            }
        }
#endif /* QAK */
    }

    my_bcomm->recv_buf = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
    //my_bcomm->irecv_reqs = malloc(my_bcomm->recv_list_len * sizeof(MPI_Request));
    my_bcomm->isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));

    MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &(my_bcomm->irecv_req));//MPI_ANY_TAG

    return 0;
}


int msg_make(void* buf_inout, int origin, int sn){
    char tmp[MSG_SIZE_MAX] = {0};
    memcpy(tmp, &origin, sizeof(int));
    memcpy(tmp + sizeof(int), &sn, sizeof(int));
    memcpy(tmp + 2 * sizeof(int), buf_inout, MSG_SIZE_MAX - 2 * sizeof(int));
    memcpy(buf_inout, &tmp, MSG_SIZE_MAX);
    return 0;
}

int msg_get_num(void* buf_in){
    //return ((int*) buf_in)[0];
    int r = 0;
    memcpy(&r, buf_in, sizeof(int));
    return r;
}

int msg_life_update(void* buf_inout){//return life before change
    //printf("before update: %s, ", buf_inout);
    int life_left;
    char tmp;
    if(strlen(buf_inout) <= 0){
        return -1;
    }
    memcpy(&tmp, buf_inout, 1);
    //assume life <= 9
    life_left = tmp - '0';
    if(life_left == 0){
        return 0;
    }
    tmp = (life_left - 1) + '0';
    memcpy(buf_inout, &tmp, 1);
    //snprintf(buf_inout, 10, "%d", life_left - 1);
    //printf("after update: %s\n", buf_inout);
    return life_left;
}
// Event progress tracking
int check_passed_origin(bcomm* my_bcomm, int origin_rank, int to_rank){
    if(to_rank == origin_rank){
        return 1;
    }

    int my_rank = my_bcomm->my_rank;

    if(my_rank >= origin_rank ){
        if(to_rank > my_rank)
            return 0;
        else {//to_rank < my_rank
            if(to_rank >= 0 && to_rank < origin_rank)
                return 0;
            else //to_rank is in [origin_rank, my_rank)
                return 1;
        }
    }else { // 0 < my_rank < origin_rank
        if(to_rank > my_rank && to_rank < origin_rank)
            return 0;
        else
            return 1;
    }
}
// Used by all ranks
int recv_forward(bcomm* my_bcomm, char* recv_buf_out) {
    //printf("3.1\n");
    //for (int i = 0; i < my_bcomm->recv_list_len; i++) {
        MPI_Status status;
        int completed = 0;
        //printf("recv_forward testing...\n");
        //printf("3.2\n");
        //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_bcomm->my_rank);
        MPI_Test(&my_bcomm->irecv_req, &completed, &status);
        //printf("3.3\n");
        if (completed) {

            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            int origin = msg_get_num(my_bcomm->recv_buf);
            //printf("recv_buf extracted data: %s, origin = %d\n", my_bcomm->recv_buf[i] + sizeof(int), origin);
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            //memset(recv_buf_out, '\0', MSG_SIZE_MAX);
            memcpy(recv_buf_out, my_bcomm->recv_buf +  sizeof(int), MSG_SIZE_MAX - sizeof(int));

            //printf("3.5\n");
            MPI_Request* isend_reqs;

            //int recv_level = get_level(my_bcomm->world_size, my_bcomm->recv_list[i]);
            int recv_level = get_level(my_bcomm->world_size, status.MPI_SOURCE);
            //if(my_bcomm->my_rank == 0)
                //printf("Rank %d received from rank %d (level %d), origin = %d, str = %s\n", my_bcomm->my_rank, status.MPI_SOURCE, recv_level, origin, (char*) (my_bcomm->recv_buf + 2*sizeof(int)));

            int send_cnt = 0;
            int upper_bound = 0;
            int tmp_my_level = get_level(my_bcomm->world_size, my_bcomm->my_rank);
            if(recv_level <= tmp_my_level){// my_bcomm->my_level; new msg, send to all channels//|| my_bcomm->my_rank != my_bcomm->world_size - 1
                upper_bound = my_bcomm->send_channel_cnt;
                //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
                //printf("new msg, set upper_bound = %d, ", upper_bound);
//                for(int j = 0; j < upper_bound; j++){
//                    printf("send_to[%d] = %d, ", j, my_bcomm->send_list[j]);
//                }
//                printf("\n");
                //if(my_bcomm->my_rank == my_bcomm->world_size - 1 || my_bcomm->my_rank == 0)
                //printf("Rank %d (level = %d) received from rank %d (level %d), origin = %d, str = %s, will forward. =====================\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, status.MPI_SOURCE, recv_level, origin, (char*) (my_bcomm->recv_buf + 2*sizeof(int)));
            }else{
                //if(my_bcomm->my_rank == my_bcomm->world_size - 1 || my_bcomm->my_rank == 0)
                //printf("Rank %d (level = %d) received from rank %d (level %d), origin = %d, str = %s, NOT forwarded. =====================\n", my_bcomm->my_rank, my_bcomm->send_channel_cnt, status.MPI_SOURCE, recv_level, origin, (char*) (my_bcomm->recv_buf + 2*sizeof(int)));
                upper_bound = my_bcomm->send_channel_cnt - 1;// not send to same level
            }
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            int lw = last_wall(my_bcomm->world_size);
            if(my_bcomm->my_rank == my_bcomm->world_size - 1){
                if(TMP_SWITCH == 1){
                    //printf("Last_wall = %d\n", lw);
                    if(!is_powerof2(my_bcomm->world_size) && status.MPI_SOURCE != lw){//&& (my_bcomm->world_size % 2 == 1)
                        upper_bound = my_bcomm->send_channel_cnt;//forward all

                        //printf("Rank world_size - 1: upper_bound = %d, will forward\n", upper_bound);
                    }
                }else{
                    //printf("Rank world_size - 1: upper_bound = %d, will NOT forward\n", upper_bound);
                }

                //printf("I'm the last rank %d, level = %d\n", my_bcomm->my_rank, my_bcomm->my_level);
                for(int j = 0; j <= upper_bound; j++){
                    //printf("send_to[%d] = %d, \n", j, my_bcomm->send_list[j]);
                }

            }

            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            int *age = (int *)(my_bcomm->recv_buf + sizeof(int));
            (*age)++;

            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);

            for(int j = 0; j <= upper_bound; j++){
//                int send_level = get_level(my_bcomm->world_size, my_bcomm->send_list[j]);
//                printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
                if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
//                    if(my_bcomm->send_list[j] != -1){
//                    printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
                        //printf("recv_forward: Rank %d received from rank %d, forward to rank %d: %s, age = %d\n", my_bcomm->my_rank, status.MPI_SOURCE, my_bcomm->send_list[j], my_bcomm->recv_buf + 2*sizeof(int), *age);
                        send_cnt++;

//                        memcpy(my_bcomm->recv_buf + sizeof(int), &age , sizeof(int));

//
//if(my_bcomm->my_rank == my_bcomm->world_size - 1 || my_bcomm->my_rank == 0)
//printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
    //printf("======Rank %d forwarding to rank %d, origin = %d, str = %s\n", my_bcomm->my_rank,my_bcomm->send_list[j], origin, (char*) (my_bcomm->recv_buf + 2*sizeof(int)));
    //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
                        MPI_Isend(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0, my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
                        //MPI_Send(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0, my_bcomm->my_comm);
//                        printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
//                    }
                }
            }
            MPI_Status isend_stat[send_cnt];
            MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
//            memset(my_bcomm->recv_buf, '\0', MSG_SIZE_MAX);

            //MPI_Irecv(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->recv_list[i], 0, my_bcomm->my_comm, &my_bcomm->irecv_reqs[i]);    //too many irecvs?
            MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &my_bcomm->irecv_req);//MPI_Irecv
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            return 0;
        }

    return -1;
}

// Used by broadcaster rank, send to send_list
int bcast(bcomm* my_comm, void* send_buf, int sn, int send_size) {
    msg_make(send_buf, my_comm->my_rank, sn);

    for (int i = 0; i < my_comm->send_list_len; i++) {
//        if(my_comm->send_list[i] != -1){
            //printf("bcast sending... on rank %d, send_buf = %s, send to rank %d\n", my_comm->my_rank, send_buf + 2*sizeof(int), my_comm->send_list[i]);
//            MPI_Request request;
//if(my_comm->send_list[i] < 0 || my_comm->send_list[i] > my_comm->world_size){
//    //printf("rank = %d, my_level = %d, send_list[%d] = %d\n", my_comm->my_rank, my_comm->my_level, i, my_comm->send_list[i]);
//}
//if(my_comm->my_rank == 0){
//    //printf("broadcasting to = %d\n", my_comm->send_list[i]);
//}
            MPI_Isend(send_buf, send_size, MPI_CHAR, my_comm->send_list[i], 0, MPI_COMM_WORLD, &my_comm->isend_reqs[i]);
            //MPI_Send(send_buf, send_size, MPI_CHAR, my_comm->send_list[i], 0, MPI_COMM_WORLD);
//        }
    }
    MPI_Status isend_stat[my_comm->send_list_len];
    MPI_Waitall(my_comm->send_list_len, my_comm->isend_reqs, isend_stat);
    return 0;
}

int bcomm_teardown(bcomm* my_bcomm){
    //for (int i = 0; i < my_bcomm->recv_list_len; i++) {//was recv_buf[i]
        MPI_Cancel(&my_bcomm->irecv_req);
    //}
    free(my_bcomm);
    return 0;
}

int random_rank(int my_rank, int world_size){
//    time_t t;
//    srand((unsigned) time(&t) + getpid());
    int next_rank = my_rank;
//    while(next_rank == my_rank){
//        next_rank = rand() % world_size;
//    }
//

//    (my_rank + 1 < world_size) ? (next_rank = my_rank + 1) : (next_rank = (my_rank + 1) % world_size);

    if(my_rank == 0)
        next_rank = world_size -1;
    else
        next_rank = my_rank - 1;

    return next_rank;
}

int test_bcast(bcomm* my_bcomm, int init_rank, int cnt) {
//    char**recv_buf = (char**)malloc(my_bcomm->recv_list_len * sizeof(char*));
//    for(int i = 0; i< my_bcomm->recv_list_len; i++){
//        recv_buf[i] = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
//    }
    char result_buf[1024];
    char* recv_buf = (char*) malloc(MSG_SIZE_MAX * sizeof(char));
    MPI_Barrier(MPI_COMM_WORLD);
    char send_buf[MSG_SIZE_MAX] = "msg";
    int num = 2 * my_bcomm->world_size;
    int sn = 0;
    int sum = 0;
    if( init_rank == my_bcomm->my_rank ){
        sprintf(send_buf, "%d", my_bcomm->my_rank);
        printf("test: Rank %d bcasting str: %s\n", my_bcomm->my_rank, send_buf);
        bcast(my_bcomm, send_buf, sn, MSG_SIZE_MAX);
    }

//    for (int i = 0; i < cnt; i++) {
//        bcast(my_bcomm, send_buf, sn, MSG_SIZE_MAX);
////        if(recv_forward(my_bcomm, recv_buf) == 0)
////            sum++;
//
//    }

    MPI_Request req;
    MPI_Status stat;
    int done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
//    for(int i = 0; i<30; i++){
//        if(recv_forward(my_bcomm, recv_buf) == 0){
//            sum++;
//        }
//    }
    while (!done) {    // extended running of recv_forward to accommodate msgs in flight

        if(recv_forward(my_bcomm, recv_buf) == 0){
            sum++;
        }
        MPI_Test(&req, &done, &stat);
        //usleep(50*1000);//100ms
    }
//    usleep(50*1000);
    done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
    while (!done) {
        if(recv_forward(my_bcomm, recv_buf) == 0){
            printf("Extra MPI_Ibarrier: Rank %d: sum = %d\n", my_bcomm->my_rank, sum);
            sum++;
        }
        MPI_Test(&req, &done, &stat);
    }

    printf("Rank %d:------------------------------ final sum = %d\n", my_bcomm->my_rank, sum);
    return 0;
}

int cleanup_Ibarrier(bcomm* my_bcomm, int cnt, int sleep_ms){
    MPI_Request req;
    MPI_Status stat;
    int done = 0;
    char recv_buf[MSG_SIZE_MAX];
    for(int i = 0; i < cnt; i++){
        done = 0;
//        usleep(sleep_ms * 1000);
        MPI_Ibarrier(my_bcomm->my_comm, &req);
        while (!done) {
            if(recv_forward(my_bcomm, recv_buf) == 0){
                printf("Extra MPI_Ibarrier\n");
            }
            MPI_Test(&req, &done, &stat);
        }
    }

    return 0;
}

void print_msg_sequence(){

}
int hacky_sack(int cnt, int starter, bcomm* my_bcomm){
    char result_buf[2*1024] = {'\0'};
    int recved = 0;
    //printf("Hacky sack start ...\n");

//    char**recv_buf = (char**)malloc(my_bcomm->recv_list_len * sizeof(char*));
//    for(int i = 0; i< my_bcomm->recv_list_len; i++){
//        recv_buf[i] = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
//    }
    char* recv_buf = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
    //recv_forward(my_bcomm, recv_buf);
    MPI_Barrier(MPI_COMM_WORLD);
    int next_rank = random_rank(my_bcomm->my_rank, my_bcomm->world_size);
    //(my_bcomm->my_rank + 1 < my_bcomm->world_size) ? (next_rank = my_bcomm->my_rank + 1) : (next_rank = (my_bcomm->my_rank + 1) % my_bcomm->world_size);


    char next_rank_str[MSG_SIZE_MAX];
    snprintf(next_rank_str, 10, "%d", next_rank);
    int sn = 0;
    //if(starter == my_bcomm->my_rank){
    //printf("rank %d, next rank is %d\n", my_bcomm->my_rank, next_rank);
        bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
    //}

    MPI_Barrier(MPI_COMM_WORLD);

    int bcast_cnt = 1;
    int num = cnt * log2(my_bcomm->world_size);
    //printf("1\n");
    int recv_rank = -1;
    for(int i = 0; bcast_cnt < cnt; i++){
        //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
        //printf("Hacky sack continue on rank %d ----------------------------------------\n", my_bcomm->my_rank);
        //printf("2\n");
        //usleep(500*1000);//5ms
        if (recv_forward(my_bcomm, recv_buf) == 0) {
            recv_rank = atoi(recv_buf + sizeof(int));
            //printf("%s:%u - rank = %d, received = %d\n", __func__, __LINE__, my_bcomm->my_rank, recv_rank);
            //printf("5.1\n");
                    // +1 to skip the first byte which is the left life time.

            //printf("5.2\n");
            int age = 0;
            recved++;
//            memcpy(&age, recv_buf, sizeof(int));
//            char origin_rank[16] = {'\0'};
//            sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
            //strcat(result_buf, origin_rank);//ranks
            //strcat(result_buf, ",");
            //printf("Rank %d recv_buf(string) = %s, age = %d, i = %d, bcast_cnt = %d\n", my_bcomm->my_rank,
            //        recv_buf + sizeof(int), age, i, bcast_cnt);
            //printf("5.3\n");
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            if (recv_rank == my_bcomm->my_rank) {            //&& bcast_cnt < cnt
                bcast_cnt++;
                //printf("%s:%u - rank = %d, bcast_cnt = %d\n", __func__, __LINE__, my_bcomm->my_rank, bcast_cnt);
                int next_rank = random_rank(my_bcomm->my_rank, my_bcomm->world_size);
                char next_rank_str[MSG_SIZE_MAX];
                snprintf(next_rank_str, 10, "%d", next_rank);            //my_bcomm->msg_life_max
                //printf("Round %d -------------- rank %d: my turn! next_rank = %d, bcast str = %s\n", bcast_cnt, my_bcomm->my_rank, next_rank, next_rank_str);
                //printf("6\n");
                sn++;
                //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
                bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
                //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
            }
            //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
        }
        //printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);
        //printf("4\n");

        //for(int j = 0; j < my_bcomm->recv_list_len; j++){
//            if(strlen(recv_buf + sizeof(int)) < 1){//origin and sn
//                continue;//skup bcast if received blank
//            }else {}
        //}
// if(my_bcomm->my_rank == 0)
//     printf("bcast_cnt = %d, cnt = %d\n", bcast_cnt, cnt);
        if(bcast_cnt >= cnt )
            break;
    }

//printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);

    MPI_Request req;
    MPI_Status stat;
    int done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
    while(!done){// extended running of recv_forward to accommodate msgs in flight
        MPI_Test(&req, &done, &stat);
        if (recv_forward(my_bcomm, recv_buf) == 0){
            int age = 0;
            recved++;
            printf("");
            recv_rank = atoi(recv_buf + sizeof(int));
            //char origin_rank[16] = {'\0'};
            //sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
            //strcat(result_buf, origin_rank);//ranks
            //strcat(result_buf, ",");

        }
        //usleep(50*1000);//100ms
    }
//    usleep(500*1000);//100ms
//printf("%s:%u - rank = %d\n", __func__, __LINE__, my_bcomm->my_rank);

    done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
    while(!done){// extended running of recv_forward to accommodate msgs in flight
        MPI_Test(&req, &done, &stat);
        if (recv_forward(my_bcomm, recv_buf) == 0){
            //printf("Extra MPI_Ibarrier: rank %d, str = %s\n", my_bcomm->my_rank, recv_buf + sizeof(int));
            int age = 0;
            recved++;
            recv_rank = atoi(recv_buf + sizeof(int));
            //char origin_rank[16] = {'\0'};
            //sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
            //strcat(result_buf, origin_rank);//ranks
            //strcat(result_buf, ",");
        }
        //usleep(500*1000);//100ms
    }

    done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
    while(!done){// extended running of recv_forward to accommodate msgs in flight
        MPI_Test(&req, &done, &stat);
        if (recv_forward(my_bcomm, recv_buf) == 0){
            //printf("Extra MPI_Ibarrier: rank %d, str = %s\n", my_bcomm->my_rank, recv_buf + sizeof(int));
            int age = 0;
            recved++;
            recv_rank = atoi(recv_buf + sizeof(int));
            //char origin_rank[16] = {'\0'};
            //sprintf(origin_rank, "%d", (recv_rank + 1)%my_bcomm->world_size);
            //strcat(result_buf, origin_rank);//ranks
            //strcat(result_buf, ",");
        }
        //usleep(500*1000);//100ms
    }

    MPI_Barrier(my_bcomm->my_comm);
    printf("Rank %d reports:  Hacky sack done, round # = %d . received %d times.  \n", my_bcomm->my_rank, bcast_cnt, recved);
    free(recv_buf);

    return 0;
}

void listLog(int n){
    for(int i = 1; i <= n; i++){
        printf("Log2(%02d) = %.3f, (int) = %d, ceil = %.1f, floor = %.1f, level = %d \n", i, log2(i),(int) log2(i), ceil(log2(i)), floor(log2(i)), get_level(n, i));
    }

}
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    bcomm* my_comm = malloc(sizeof(bcomm));

    if(routing_table_init(my_comm, MPI_COMM_WORLD) != 0){
        return 0;
    }

    int game_cnt = atoi(argv[2]);
    //DEBUG_MODE = argv[3][0];
    int init_rank = atoi(argv[1]);
    TMP_SWITCH = atoi(argv[1]);
    //log_init(MPI_COMM_WORLD, MY_LOG);
    //printf("input is power of 2\n");
    //debug(MY_LOG);
    MPI_Barrier(MPI_COMM_WORLD);


//    int world_size = atoi(argv[1]);
//    int my_rank = atoi(argv[2]);
//    printf("world_size = %d, my_rank = %d, my_level = %d\n", world_size, my_rank, get_level(world_size, my_rank));
    //test_bcast(my_comm, init_rank, game_cnt);
    hacky_sack(game_cnt, init_rank, my_comm);


    //makeTable();
//    while(init_rank != 1 && init_rank % 2 == 0){
//        init_rank >>= 1;
//    }
//    if(init_rank == 1)
//        printf("input is power of 2\n");
//    else
//        printf("input is not power of 2\n");

    bcomm_teardown(my_comm);
    //log_close(MY_LOG);
    MPI_Finalize();
    return 0;
}
