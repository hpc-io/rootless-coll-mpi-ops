#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <unistd.h>
#include <time.h>

#define MAIL_BAG_SIZE 4
#define MSG_SIZE_MAX 64

typedef struct Mail_entry {
    char* message;
} mail;

typedef struct Mail_bag {
    unsigned int mail_cnt;
    mail* bag;
} mailbag;

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
    MPI_Request* irecv_reqs;
    MPI_Request* isend_reqs;
    char** recv_buf;//2D array

} bcomm;

int get_level(int n, int rank){
    if(rank == 0)
        return log2(n) - 1;
    int l = 0;
    while(rank % 2 == 0){
        rank >>= 1;
        l++;
    }
    return l;
}
int routing_table_init(bcomm* my_bcomm, MPI_Comm comm) {
    my_bcomm->my_comm = comm;
    MPI_Comm_size(my_bcomm->my_comm, &my_bcomm->world_size);
    MPI_Comm_rank(my_bcomm->my_comm, &my_bcomm->my_rank);
    my_bcomm->my_level = get_level(my_bcomm->world_size, my_bcomm->my_rank);
    my_bcomm->msg_life_max = (int) log2(my_bcomm->world_size) + 1; //(int) log2(my_bcomm->world_size);
    my_bcomm->recv_list_len = my_bcomm->my_level + 1;
    my_bcomm->recv_list = malloc(my_bcomm->recv_list_len * sizeof(int));
    my_bcomm->send_list_len = my_bcomm->my_level + 1;
    my_bcomm->send_list = malloc(my_bcomm->send_list_len * sizeof(int));

    for(int i = 0; i <= my_bcomm->my_level; i++){
        my_bcomm->send_list[i] = (int)(my_bcomm->my_rank + pow(2, i)) %  my_bcomm->world_size;
        my_bcomm->recv_list[i] = (int)(my_bcomm->my_rank - pow(2, i) + my_bcomm->world_size) %  my_bcomm->world_size;
    }
    //my_bcomm->send_list_right[0] = (2 * my_bcomm->my_rank + 1) % world_size;
    //my_bcomm->send_list_right[1] = (2 * my_bcomm->my_rank + 2) % world_size;

    my_bcomm->recv_buf = (char**)malloc(my_bcomm->recv_list_len * sizeof(char*));
    for(int i = 0; i< my_bcomm->recv_list_len; i++){
        my_bcomm->recv_buf[i] = (char*)malloc(MSG_SIZE_MAX * sizeof(char));
    }

    my_bcomm->irecv_reqs = malloc(my_bcomm->recv_list_len * sizeof(MPI_Request));
    my_bcomm->isend_reqs = malloc(my_bcomm->send_list_len * sizeof(MPI_Request));
    printf("routing_table_init loop MPI_Irecv...\n");
    for (int i = 0; i < my_bcomm->recv_list_len; i++) {//was recv_buf[i]
        //printf("recv_forward recving at rank %d\n", my_comm->my_rank);
        MPI_Irecv(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->recv_list[i],
                0, my_bcomm->my_comm, &my_bcomm->irecv_reqs[i]);
    }

    return 0;
}

//Message doubling: works like message broadcasts from the root down to leaves, log(n) time, no duplicated msg, single recv_from.
//Problem: no ack.

int MsgTag_read(char buf_in[]){
    int tag = 0;
    return tag;
}

int MsgTag_update(char buf_inout[], int new_tag){
    return 0;
}

int msg_extract(char buf_inout[], int* tag_out, int* src_rank){
    return 0;
}

int msg_make(void* buf_inout, int num){
    char tmp[MSG_SIZE_MAX] = {0};
    memcpy(tmp, &num, sizeof(int));
    memcpy(tmp + sizeof(int), buf_inout, MSG_SIZE_MAX - sizeof(int));
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
    if(my_bcomm->my_rank > origin_rank){
        if(to_rank < origin_rank + my_bcomm->world_size){
            return 0;
        }else{
            return 1;
        }
    } else {
        if(to_rank < origin_rank){
            return 0;
        }else{
            return 1;
        }
    }
}
// Used by all ranks
int recv_forward(bcomm* my_bcomm, void* recv_buf_out) {
    //printf("recv_forward start\n");
    //MPI_Barrier(my_bcomm->my_comm);
    for (int i = 0; i < my_bcomm->recv_list_len; i++) {
        MPI_Status status;
        int completed = 0;
        //printf("recv_forward testing...\n");
        MPI_Test(&my_bcomm->irecv_reqs[i], &completed, &status);
        if (completed) {
            int origin = msg_get_num(my_bcomm->recv_buf[i]);
            //printf("recv_buf extracted data: %s, origin = %d\n", my_bcomm->recv_buf[i] + sizeof(int), origin);

            memcpy(recv_buf_out, my_bcomm->recv_buf[i] + sizeof(int), MSG_SIZE_MAX);

            MPI_Request* isend_reqs;

            int recv_level = get_level(my_bcomm->world_size, my_bcomm->recv_list[i]);




            //================== Refactoring for() start==================
            int send_cnt = 0;
            for(int j = 0; j < my_bcomm->send_list_len; j++){
                int send_level = get_level(my_bcomm->world_size, my_bcomm->send_list[j]);

                if(recv_level < my_bcomm->my_level){//must also forward to same ring
                    if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
                       printf("Rank %d forward new msg to rank %d: %s\n", my_bcomm->my_rank, my_bcomm->send_list[j], my_bcomm->recv_buf[i] + sizeof(int));
                        send_cnt++;
                        MPI_Isend(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0,
                                my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
                    }
                }else{//forward to lower rings
                    if (my_bcomm->my_level > send_level){
                        if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
                            printf("Rank %d forward to rank %d: %s\n", my_bcomm->my_rank, my_bcomm->send_list[j], my_bcomm->recv_buf[i] + sizeof(int));
                            send_cnt++;
                            MPI_Isend(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0,
                                    my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
                        }
                    }
                }
            }
            MPI_Status isend_stat[send_cnt];
            MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);
            //================== Refactoring for() loop end==================

//            if (recv_level < my_bcomm->my_level) { //must also forward to same ring: top ring level of last rank: new msg,
//                int send_cnt = 0;
//                for (int j = 0; j < my_bcomm->send_list_len; j++) { // only send to lower rings (i-1), unless it's a new msg (i)
////                    printf(
////                            "received a new msg!! ----------recv_forward on rank %d (level %d), send_buf = %s, from rank %d (level %d) \n",
////                            my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->recv_buf[i] + sizeof(int),
////                            my_bcomm->recv_list[i], get_level(my_bcomm->world_size, my_bcomm->recv_list[i]));
//                    //if(get_level(my_bcomm->world_size, my_bcomm->my_rank) != 0){//leaf node won't forward
//                    if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
//                        printf("send to rank %d, origin+world = %d\n", my_bcomm->send_list[j] , origin + my_bcomm->world_size);
//                        send_cnt++;
//                        MPI_Isend(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0,
//                                my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
//                    }
//                }
//                MPI_Status isend_stat[send_cnt];
//                MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);    // sync
//            } else {    // recv_level > my_bcomm->my_level : otherwise only forward to lower rings
//                int j = 0;
//                int send_cnt = 0;
//                for (j = 0; j < my_bcomm->send_list_len - 1; j++) { // only send to lower rings (i-1), unless it's a new msg (i)
//                    //char tmp[MSG_SIZE_MAX];// = my_bcomm->recv_buf[i];
//                    //memcpy(tmp, my_bcomm->recv_buf[i], MSG_SIZE_MAX);
//                    int send_level = get_level(my_bcomm->world_size, my_bcomm->send_list[j]);
//                    if (my_bcomm->my_level > send_level) {
//                        //printf("my level = %d, send_level = %d, sending...\n", my_bcomm->my_level,  send_level);
////                        printf(
////                                "------recv_forward on rank %d (level %d), send_buf = %s, from rank %d (level %d)\n",
////                                my_bcomm->my_rank, my_bcomm->my_level, my_bcomm->recv_buf[i] + sizeof(int),
////                                my_bcomm->recv_list[i],
////                                get_level(my_bcomm->world_size, my_bcomm->recv_list[i]));
//                        if (check_passed_origin(my_bcomm, origin, my_bcomm->send_list[j]) == 0) {
//                            printf("send to rank %d, origin+world = %d\n", my_bcomm->send_list[j] , origin + my_bcomm->world_size);
//                            send_cnt++;
//                            MPI_Isend(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->send_list[j], 0,
//                                    my_bcomm->my_comm, &(my_bcomm->isend_reqs[j]));    //&my_bcomm->isend_reqs[j]
//                        }
//                    }
//                }
//                MPI_Status isend_stat[send_cnt];
//                MPI_Waitall(send_cnt, my_bcomm->isend_reqs, isend_stat);    // sync
//            }

            MPI_Irecv(my_bcomm->recv_buf[i], MSG_SIZE_MAX, MPI_CHAR, my_bcomm->recv_list[i], 0, my_bcomm->my_comm,
                    &my_bcomm->irecv_reqs[i]);    //too many irecvs?
        }
    }

    return 0;
}

// Used by broadcaster rank
int bcast(bcomm* my_comm, void* send_buf, int send_size) {
    //send to send_list_left and send_list_right
    //printf("bcast 1\n");
    msg_make(send_buf, my_comm->my_rank );
    for (int i = 0; i < my_comm->send_list_len; i++) {
        //printf("bcast sending... on rank %d, send_buf = %s, send to rank %d\n", my_comm->my_rank, send_buf + sizeof(int), my_comm->send_list[i]);
        MPI_Request request;
        MPI_Isend(send_buf, send_size, MPI_CHAR, my_comm->send_list[i],
                0, MPI_COMM_WORLD, &my_comm->isend_reqs[i]);
        //printf("bcast 3\n");
    }
    return 0;
}

int bcomm_teardown(bcomm* my_bcomm){
    for (int i = 0; i < my_bcomm->recv_list_len; i++) {//was recv_buf[i]
        //MPI_Barrier(my_bcomm->my_comm);
        //printf("MPI_Cancel: rank = %d, i = %d\n",my_bcomm->my_rank, i);
        MPI_Cancel(&my_bcomm->irecv_reqs[i]);
    }
    free(my_bcomm);
    return 0;
}

int random_rank(int my_rank, int world_size){
    time_t t;
    srand((unsigned) time(&t) + getpid());
    int next_rank = my_rank;
    while(next_rank == my_rank){
        next_rank = rand() % world_size;
    }
    return next_rank;
}

int test_bcast(bcomm* my_bcomm, int init_rank, int cnt){
    char recv_buf[MSG_SIZE_MAX] = {0};
    char send_buf[MSG_SIZE_MAX] = {0};
    int num = 2 * my_bcomm->world_size;

    if( init_rank == my_bcomm->my_rank ){
        sprintf(send_buf, "%d", my_bcomm->my_rank);
        printf("test: Rank %d bcasting str: %s\n", my_bcomm->my_rank, send_buf);
        bcast(my_bcomm, send_buf, MSG_SIZE_MAX);
    }
    //sleep(1);
    MPI_Barrier(MPI_COMM_WORLD);

    for(int j = 0; j < cnt; j++){

        for(int i = 0; i < num; i++){
            //MPI_Barrier(MPI_COMM_WORLD);
            usleep(300 * 1000);
            recv_forward(my_bcomm, recv_buf);
            if(strlen(recv_buf) != 0){
                printf("test_bcast:recv_forward: Rank %d received str: %s\n", my_bcomm->my_rank, recv_buf);
            }
        }
//        init_rank++;
//        if( init_rank == my_bcomm->my_rank ){
//            sprintf(send_buf, "%d", my_bcomm->my_rank);
//            printf("test: Rank %d bcasting str: %s\n", my_bcomm->my_rank, send_buf);
//            bcast(my_bcomm, send_buf, MSG_SIZE_MAX);
//        }
    }

    return 0;
}



int hacky_sack(int cnt, int starter, bcomm* my_bcomm){
    printf("Hacky sack start ...\n");


    //printf("Rank %d recv from %d and send to %d\n", my_comm->my_rank, my_comm->recv_list_left[0], my_comm->send_list_right[0]);
    if(my_bcomm->my_rank == starter | my_bcomm->my_rank == starter + 1 | my_bcomm->my_rank == starter + 2){
        int next_rank = random_rank(my_bcomm->my_rank, my_bcomm->world_size);
        printf("I'm the starter: rank %d, next rank is %d\n", my_bcomm->my_rank, next_rank);
        char next_rank_str[MSG_SIZE_MAX];
        snprintf(next_rank_str, 10, "%d", next_rank);
        bcast(my_bcomm, next_rank_str, MSG_SIZE_MAX);
    }
    int t_ms = 1;
    usleep(500 * 1000);
    //void* recv_buf = calloc(MSG_SIZE_MAX, sizeof(char));
    char recv_buf[MSG_SIZE_MAX] = {0};
    //printf("Initial strlen(recv_buf) = %lu\n", strlen(recv_buf));
    //void* recv_buf = calloc(1, sizeof(int));
    int bcast_cnt = 0;
    int num = cnt * log2(my_bcomm->world_size) ;
    for(int i = 0; i < num; i++){
        //printf("Hacky sack cnt = %d\n", bcast_cnt);
        usleep(t_ms * 1000);//

        //printf("Hacky sack continue on rank %d ----------------------------------------\n", my_comm->my_rank);
        memset(recv_buf, '\0', MSG_SIZE_MAX);
        recv_forward(my_bcomm, recv_buf);

        printf("Rank %d recv_buf(string) =  %s\n", my_bcomm->my_rank, recv_buf);
        int recv_rank = -1;
        if(strlen(recv_buf) < 1){
            //recv_rank = -1;
            continue;//skup bcast if received blank
        }else{
            recv_rank = atoi(recv_buf);// +1 to skip the first byte which is the left life time.
            printf("atoi(recv_buf) = %d\n", recv_rank);
        }

        if(recv_rank == my_bcomm->my_rank){
            bcast_cnt++;
            int next_rank = random_rank(my_bcomm->my_rank, my_bcomm->world_size);
            char next_rank_str[MSG_SIZE_MAX];
            snprintf(next_rank_str, 10, "%d", next_rank);//my_bcomm->msg_life_max
            printf("Round %d --------------rank %d: my turn! next_rank = %d, bcast str = %s\n", bcast_cnt, my_bcomm->my_rank, next_rank, next_rank_str);
            bcast(my_bcomm, next_rank_str, MSG_SIZE_MAX);
//            bcast_cnt++;
//            printf("After ++: bcast_cnt = %d\n", bcast_cnt);
        }else{
            //recv_forward(my_bcomm, recv_buf);
            //clear buf.
//            int life_left = msg_life_update(recv_buf);
//            if(life_left > 0){
//                bcast(my_bcomm, recv_buf, MSG_SIZE_MAX);
//            }
            // should continue
        }
    }
    printf("Hacky sack done.\n");
    //free(recv_buf);
    return 0;
}

//tested
int rma_mailbag_get(void *buf_recv, int mail_count, int mail_size,
        int target_rank, MPI_Aint target_offset, MPI_Win target_win,
        int default_lock) {
    int get_size = mail_count * mail_size;
    if (default_lock) {
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, target_rank, 0, target_win); //MPI_LOCK_EXCLUSIVE, MPI_LOCK_SHARED
    }

    MPI_Get(buf_recv, 2 * get_size, MPI_CHAR, target_rank, 0,
            2 * MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, target_win);

    if (default_lock) {
        MPI_Win_unlock(target_rank, target_win);
    }
    return 0;
}

//tested
int rma_mailbag_put(const void *buf_src, int mail_count, int mail_size,
        int target_rank, MPI_Aint target_offset, MPI_Win target_win,
        int default_lock) {
    int put_size = mail_count * mail_size;
    if (default_lock) {
        MPI_Win_lock(MPI_LOCK_EXCLUSIVE, target_rank, 0, target_win); //MPI_LOCK_EXCLUSIVE, MPI_LOCK_SHARED
    }

    MPI_Put(buf_src, put_size, MPI_CHAR, target_rank, target_offset, put_size,
            MPI_CHAR, target_win);

    if (default_lock) {
        MPI_Win_unlock(target_rank, target_win);
    }
    return 0;
}

void makeTable(void){

    //int** table = malloc(n * level_cnt*sizeof(int));//
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
    routing_table_init(my_comm, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    int game_cnt = atoi(argv[2]);

    int init_rank = atoi(argv[1]);
    //test_bcast(my_comm, init_rank, game_cnt);
    hacky_sack(game_cnt, init_rank, my_comm);

//    for(int i = 0; i < 32; i++){
//        printf("i = %d, level = %d\n", i, get_level(32, i));
//    }
    bcomm_teardown(my_comm);

    char tmp[MSG_SIZE_MAX] = {"987654321987654321987654321"};
    int n = 12;
    //memcpy(tmp, &n, sizeof(int));

    //memcpy(&n2, tmp, sizeof(int));
    //msg_make(tmp, n);
    //int n2 = msg_get_num(tmp);
    //printf("n = %d, tmp = %s, n2 = %d\n", n, tmp + sizeof(int), n2);
    MPI_Finalize();
    //free(my_comm);
    return 0;
}
