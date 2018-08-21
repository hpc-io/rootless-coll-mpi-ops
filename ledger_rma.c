#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <time.h>

#include <pthread.h>


#define MSG_SIZE_MAX 64

typedef struct BCastCommunicator {
    MPI_Comm my_comm;
    int my_rank;
    int my_level;
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
    MPI_Request irecv_reqs;
    MPI_Request* isend_reqs;
    char* recv_buf;
    //char** recv_buf;//2D array

} bcomm;

int is_powerof2(int n){
    while(n != 1 && n % 2 == 0){
        n >>= 1;
    }
    if(n == 1){
        //printf("world_size is power of 2\n");
        return 1;
    } else {
        //printf("world_size is not power of 2\n");
        return 0;
    }
}

int get_level(int world_size, int rank){
    if(is_powerof2(world_size)){
        if(rank == 0){
            return log2(world_size) - 1;
        }
    }else{
        if(rank == 0)
            return log2(world_size);
//        if(rank ==world_size - 1)
//            return 0;
    }

    int l = 0;
    while(rank != 0 && rank % 2 == 0){
        rank >>= 1;
        l++;
    }
    return l;
}

int routing_table_init(bcomm* my_bcomm, MPI_Comm comm) {
    my_bcomm->my_comm = comm;
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    if(my_bcomm->world_size < 2){
        printf("Too few ranks, program ended. world_size = %d\n", my_bcomm->world_size);
        return -1;
    }

    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);
    my_bcomm->my_level = get_level(my_bcomm->world_size, my_bcomm->my_rank);
    my_bcomm->msg_life_max = (int) log2(my_bcomm->world_size) + 1; //(int) log2(my_bcomm->world_size);

    //int size = my_bcomm->world_size;
    int is_power2 = is_powerof2(my_bcomm->world_size);

    if(is_power2){//OK
        my_bcomm->recv_list_len = my_bcomm->my_level + 1;
        my_bcomm->send_list_len = my_bcomm->my_level + 1;

        //my_bcomm->recv_list = malloc(my_bcomm->recv_list_len * sizeof(int));
        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for(int i = 0; i <= my_bcomm->my_level; i++){
            my_bcomm->send_list[i] = (int)(my_bcomm->my_rank + pow(2, i)) %  my_bcomm->world_size;
            //my_bcomm->recv_list[i] = (int)(my_bcomm->my_rank - pow(2, i) + my_bcomm->world_size) %  my_bcomm->world_size;
            printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i]);
        }
    }else{
        my_bcomm->recv_list_len = my_bcomm->my_level + 1;
        my_bcomm->send_list_len = my_bcomm->my_level + 1;

        //my_bcomm->recv_list = malloc(my_bcomm->recv_list_len * sizeof(int));
        my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

        for(int i = 0; i <= my_bcomm->my_level; i++){
            my_bcomm->send_list[i] = (int)(my_bcomm->my_rank + pow(2, i)) %  my_bcomm->world_size;
            //my_bcomm->recv_list[i] = (int)(my_bcomm->my_rank - pow(2, i) + my_bcomm->world_size) %  my_bcomm->world_size;
            if(my_bcomm->my_rank != my_bcomm->world_size - 1)
                printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i]);
        }
        if(my_bcomm->my_rank == my_bcomm->world_size - 1){//patch for rank n-1
            my_bcomm->send_list[0] = 0;
            printf("Rank %d, level = %d send_to %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[0]);
            for(int i = 1; i < my_bcomm->my_level; i++){
                my_bcomm->send_list[i] = -1;
                printf("Rank %d, level = %d send_to %dn", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i]);
            }
        }
    }

//    my_bcomm->recv_list_len = my_bcomm->my_level + 1;
//    my_bcomm->send_list_len = my_bcomm->my_level + 1;
//
//    my_bcomm->recv_list = malloc(my_bcomm->recv_list_len * sizeof(int));
//    my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));
//
//    for(int i = 0; i <= my_bcomm->my_level; i++){
//        my_bcomm->send_list[i] = (int)(my_bcomm->my_rank + pow(2, i)) %  my_bcomm->world_size;
//        my_bcomm->recv_list[i] = (int)(my_bcomm->my_rank - pow(2, i) + my_bcomm->world_size) %  my_bcomm->world_size;
//        printf("Rank %d, level = %d send_to %d, recv from %d\n", my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->send_list[i], my_bcomm->recv_list[i]);
//    }
    //my_bcomm->send_list_right[0] = (2 * my_bcomm->my_rank + 1) % world_size;
    //my_bcomm->send_list_right[1] = (2 * my_bcomm->my_rank + 2) % world_size;

//    my_bcomm->recv_buf = (char**)malloc(my_bcomm->recv_list_len * sizeof(char*));
//    for(int i = 0; i< my_bcomm->recv_list_len; i++){
//        my_bcomm->recv_buf[i] = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
//    }
    my_bcomm->recv_buf = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
    //my_bcomm->irecv_reqs = malloc(my_bcomm->recv_list_len * sizeof(MPI_Request));
    my_bcomm->isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    printf("routing_table_init loop MPI_Irecv...\n");
    MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &(my_bcomm->irecv_reqs));//MPI_ANY_TAG
//    for (int i = 0; i < my_bcomm->recv_list_len; i++) {//was recv_buf[i]
//        //printf("recv_forward recving at rank %d\n", my_comm->my_rank);
//        MPI_Irecv(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->recv_list[i],
//                0, my_bcomm->my_comm, &my_bcomm->irecv_reqs[i]);
//    }

    return 0;
}

//Message doubling: works like message broadcasts from the root down to leaves, log(n) time, no duplicated msg, single recv_from.
//Problem: no ack.


int msg_make(void* buf_inout, int origin, int sn){
    char tmp[MSG_SIZE_MAX] = {0};
    memcpy(tmp, &origin, sizeof(int));
    memcpy(tmp + sizeof(int), &sn, sizeof(int));
    memcpy(tmp + 2*sizeof(int), buf_inout, MSG_SIZE_MAX - 2 * sizeof(int));
    memcpy(buf_inout, &tmp, MSG_SIZE_MAX);
    return 0;
}

int msg_get_num(void* buf_in){
    return ((int*) buf_in)[0];
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
//    if(my_bcomm->my_rank > origin_rank){
//        if(to_rank < origin_rank + my_bcomm->world_size){
//            return 0;
//        }else{
//            return 1;
//        }
//    } else {
//        if(to_rank < origin_rank){
//            return 0;
//        }else{
//            return 1;
//        }
//    }
}
// Used by all ranks
int recv_forward(bcomm* my_bcomm, char* recv_buf_out) {
    //printf("3.1\n");
    //for (int i = 0; i < my_bcomm->recv_list_len; i++) {
        MPI_Status status;
        int completed = 0;
        //printf("recv_forward testing...\n");
        //printf("3.2\n");
        MPI_Test(&my_bcomm->irecv_reqs, &completed, &status);
        //printf("3.3\n");
        if (completed) {
            int origin = msg_get_num(my_bcomm->recv_buf);
            //printf("recv_buf extracted data: %s, origin = %d\n", my_bcomm->recv_buf[i] + sizeof(int), origin);
            //printf("3.4\n");
            memcpy(recv_buf_out, my_bcomm->recv_buf +  sizeof(int), MSG_SIZE_MAX);
            //recv_buf_out[i] = my_bcomm->recv_buf[i] + sizeof(int);
            //printf("3.5\n");
            MPI_Request* isend_reqs;

            //int recv_level = get_level(my_bcomm->world_size, my_bcomm->recv_list[i]);
            int recv_level = get_level(my_bcomm->world_size, status.MPI_SOURCE);

            //================== Refactoring for() start==================
            int send_cnt = 0;
            int upper_bound = 0;
            if(recv_level < my_bcomm->my_level){// new msg, send to all channels
                upper_bound = my_bcomm->send_list_len;
            }else{
                upper_bound = my_bcomm->send_list_len - 1;// not send to same level
            }

            for(int j = 0; j < upper_bound; j++){
                int send_level = get_level(my_bcomm->world_size, my_bcomm->send_list[j]);

                if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
                    if(my_bcomm->send_list[j] != -1){
                        printf("Rank %d forward to rank %d: %s\n", my_bcomm->my_rank, my_bcomm->send_list[j], my_bcomm->recv_buf + 2*sizeof(int));
                        send_cnt++;
                        MPI_Isend(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0,
                                my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
                    }
                }
            }
            MPI_Status isend_stat[send_cnt];
            MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);
            memset(my_bcomm->recv_buf, '\0', MSG_SIZE_MAX);

            //MPI_Irecv(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->recv_list[i], 0, my_bcomm->my_comm, &my_bcomm->irecv_reqs[i]);    //too many irecvs?
            MPI_Irecv(my_bcomm->recv_buf, MSG_SIZE_MAX, MPI_CHAR, MPI_ANY_SOURCE, 0, my_bcomm->my_comm, &my_bcomm->irecv_reqs);//MPI_Irecv
        }

    return 0;
}

// Used by broadcaster rank, send to send_list_left and send_list_right
int bcast(bcomm* my_comm, void* send_buf, int sn, int send_size) {
    msg_make(send_buf, my_comm->my_rank, sn);

    for (int i = 0; i < my_comm->send_list_len; i++) {
        if(my_comm->send_list[i] != -1){
            printf("bcast sending... on rank %d, send_buf = %s, send to rank %d\n", my_comm->my_rank, send_buf + 2*sizeof(int), my_comm->send_list[i]);
            MPI_Request request;
            MPI_Isend(send_buf, send_size, MPI_CHAR, my_comm->send_list[i],
                    0, MPI_COMM_WORLD, &my_comm->isend_reqs[i]);
        }
    }
    return 0;
}

int bcomm_teardown(bcomm* my_bcomm){
    //for (int i = 0; i < my_bcomm->recv_list_len; i++) {//was recv_buf[i]
        MPI_Cancel(&my_bcomm->irecv_reqs);
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

    //(my_rank + 1 < world_size) ? (next_rank = my_rank + 1) : (next_rank = (my_rank + 1) % world_size);

    if(my_rank == 0)
        next_rank = world_size -1;
    else
        next_rank = my_rank - 1;
    return next_rank;
}

int test_bcast(bcomm* my_bcomm, int init_rank, int cnt){
//    char**recv_buf = (char**)malloc(my_bcomm->recv_list_len * sizeof(char*));
//    for(int i = 0; i< my_bcomm->recv_list_len; i++){
//        recv_buf[i] = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
//    }
    char* recv_buf = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
    MPI_Barrier(MPI_COMM_WORLD);
    char send_buf[MSG_SIZE_MAX] = {0};
    int num = 2 * my_bcomm->world_size;
    int sn = 0;
    if( init_rank == my_bcomm->my_rank ){
        sprintf(send_buf, "%d", my_bcomm->my_rank);
        printf("test: Rank %d bcasting str: %s\n", my_bcomm->my_rank, send_buf);
        bcast(my_bcomm, send_buf, sn, MSG_SIZE_MAX);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    //for(int j = 0; j < cnt; j++){

        for(int i = 0; i < num; i++){
            //MPI_Barrier(MPI_COMM_WORLD);
            usleep(300 * 1000);
            recv_forward(my_bcomm, recv_buf);
            //for(int j = 0; j < my_bcomm->recv_list_len; j++){
                //if(strlen(recv_buf[j]+1) != 0){
                    printf("test_bcast:recv_forward: Rank %d received str: %s\n", my_bcomm->my_rank, recv_buf+1);
                //}
            //}
        }
    //}
    return 0;
}



int hacky_sack(int cnt, int starter, bcomm* my_bcomm){
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
    if(starter == my_bcomm->my_rank){
        printf("I'm the starter: rank %d, next rank is %d\n", my_bcomm->my_rank, next_rank);
        bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    int bcast_cnt = 0;
    int num = cnt * log2(my_bcomm->world_size) ;
    //printf("1\n");
    for(int i = 0; bcast_cnt < cnt; i++){
        //printf("Hacky sack continue on rank %d ----------------------------------------\n", my_comm->my_rank);
        //printf("2\n");
        //usleep(500*1000);//5ms
        recv_forward(my_bcomm, recv_buf);
        //printf("4\n");
        int recv_rank = -1;
        //for(int j = 0; j < my_bcomm->recv_list_len; j++){
            if(strlen(recv_buf + sizeof(int)) < 1){//origin and sn
                continue;//skup bcast if received blank
            }else {
                //printf("5.1\n");
                recv_rank = atoi(recv_buf+ sizeof(int));// +1 to skip the first byte which is the left life time.
                //printf("5.2\n");
                int recv_sn = 0;
                memcpy(&recv_sn, recv_buf, sizeof(int));
                printf("Rank %d recv_buf(string) = %s, recv_sn = %d, i = %d, bcast_cnt = %d\n", my_bcomm->my_rank, recv_buf + sizeof(int), recv_sn, i, bcast_cnt);
                //printf("5.3\n");
                if(recv_rank == my_bcomm->my_rank && bcast_cnt < cnt){//&& bcast_cnt < cnt
                    bcast_cnt++;
                    int next_rank = random_rank(my_bcomm->my_rank, my_bcomm->world_size);
                    char next_rank_str[MSG_SIZE_MAX];
                    snprintf(next_rank_str, 10, "%d", next_rank);//my_bcomm->msg_life_max
                    printf("Round %d -------------- rank %d: my turn! next_rank = %d, bcast str = %s\n", bcast_cnt, my_bcomm->my_rank, next_rank, next_rank_str);
                    //printf("6\n");
                    sn++;
                    bcast(my_bcomm, next_rank_str, sn, MSG_SIZE_MAX);
                }
            }
        //}
        if(bcast_cnt >= cnt ){
            break;
        }
    }
    MPI_Request req;
    MPI_Status stat;
    int done = 0;
    MPI_Ibarrier(my_bcomm->my_comm, &req);
    while(!done){// extended running of recv_forward to accommodate msgs in flight
        MPI_Test(&req, &done, &stat);
        recv_forward(my_bcomm, recv_buf);
        usleep(50*1000);//100ms
    }

        printf("Rank %d reports:  ================= Hacky sack done, round # = %d . =================\n", my_bcomm->my_rank, bcast_cnt);
    free(recv_buf);
    return 0;
}



void makeTable(void){

    int table[4][16] = {0};

    int n=16;
    int level_cnt = (int)log2(n);
    for(int i = 0; i < level_cnt; i++){
        for(int j = 0; j < n; j++){
            table[i][j] = -1;
        }
    }
    for(int level = 0; level < level_cnt; level ++){
        int skip = pow(2, level);
        printf("Level %d: ", level);
        for(int rank = 0; rank < n; rank += skip){
            table[level][rank]=rank;
            printf("%d  ", rank);
        }
        printf("\n");

    }
    printf("\n\n");
    for(int i = 0; i < level_cnt; i++){
        printf("Level %d: ", i);
        for(int j = 0; j < n; j++){
            printf("%d  ", table[i][j]);
        }
        printf("\n");
    }
    //free(table);
}
int main(int argc, char** argv) {
    MPI_Init(NULL, NULL);
    bcomm* my_comm = malloc(sizeof(bcomm));

    if(routing_table_init(my_comm, MPI_COMM_WORLD) != 0){
        return 0;
    }

    MPI_Barrier(MPI_COMM_WORLD);
    int game_cnt = atoi(argv[2]);

    int init_rank = atoi(argv[1]);
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
    MPI_Finalize();
    return 0;
}
