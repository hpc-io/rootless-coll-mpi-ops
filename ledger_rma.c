#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#define MAIL_BAG_SIZE 4
#define MSG_SIZE_MAX 64

typedef struct Mail_entry{
    char* message;
}mail;

typedef struct Mail_bag{
    unsigned int mail_cnt;
    mail* bag;
}mailbag;

int main(int argc, char** argv) {
    // Initialize the MPI environment

    char buf_put[MAIL_BAG_SIZE][MSG_SIZE_MAX] = {' '};
    char buf_get[2 * MAIL_BAG_SIZE][MSG_SIZE_MAX] = {' '};
    unsigned int WIN_SIZE_MIN = MAIL_BAG_SIZE * MSG_SIZE_MAX * (sizeof(char)) + sizeof(unsigned int);
    MPI_Init(NULL, NULL);

    // Get the number of processes

    /* create private memory */
    int* shared_int;
    int rank_shared = 0;
    /* collectively declare memory as remotely accessible */
    MPI_Win my_win;
    MPI_Win_allocate(2 * MAIL_BAG_SIZE * MSG_SIZE_MAX * sizeof(char), sizeof(char), MPI_INFO_NULL, MPI_COMM_WORLD, &shared_int, &my_win);
    //first half for get, second half for put
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    if(world_size < 2){
        printf("Too less processes launched: must be at least 2.");
        return 0;
    }

    // Get the rank of the process
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);

    // Print off a hello world message
    printf("Hello from processor %s, rank %d out of %d processors\n",
            processor_name, my_rank, world_size);

    //initialize a var in local window, set to rank.

    mailbag my_bag;
    my_bag.mail_cnt = 0;
    my_bag.bag = malloc(MAIL_BAG_SIZE * sizeof(mail));
    printf("Filling mailbags at rank_%d: \n", my_rank);
    for(int i = 0; i < MAIL_BAG_SIZE; i++){
        my_bag.bag[i].message = malloc(MSG_SIZE_MAX*sizeof(char));
        sprintf((my_bag.bag[i].message), "msg%d_rank%d", i, my_rank);//mailbag struct
        sprintf(buf_put[i], "msg%d_rank%d", i, my_rank);//plain 2D char array
    }
    //memset(my_bag.bag, '-', MSG_SIZE_MAX*MAIL_BAG_SIZE);

    printf("Print mailbag from rank %d: \n", my_rank);
    for(int i = 0; i < MAIL_BAG_SIZE; i++){
        //printf("%s\n", my_bag.bag[i].message);
        printf("buf_put at rank %d: %s\n", my_rank, buf_put[i]);
    }

    MPI_Win_fence(0, my_win);//collective sync open
    MPI_Put(&buf_put, MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, my_rank, 0 , MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, my_win);
    MPI_Win_fence(0, my_win);//sync close

    int recv = 0;
    int rank_next = my_rank + 1;
    if(my_rank == world_size - 1) {
        rank_next = 0;
    }
    printf("My rank = %d, next rank = %d\n", my_rank, rank_next);
    //MPI_Win_fence(0, my_win);//collective sync open
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank_next, 0, my_win);//MPI_LOCK_SHARED
    //MPI_Win_fence(0, my_win);
    MPI_Put(&buf_put, MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, rank_next, MAIL_BAG_SIZE * MSG_SIZE_MAX, MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, my_win);
    //read self, and expect to also get msg from previous rank
    MPI_Win_unlock(rank_next, my_win);
    //MPI_Win_fence(0, my_win);

    //printf("my rank = %d, read rank_0 content = %d\n", my_rank, recv);
    //MPI_Put(&my_rank, 1, MPI_INT, rank_shared, 0 ,1, MPI_INT, my_win);
//    MPI_Win_fence(0, my_win);//sync close
//
//    MPI_Win_fence(0, my_win);//sync open
    //MPI_Get(&recv, 1, MPI_INT, rank_shared, 0, 1, MPI_INT, my_win);
    //printf("After put: my rank = %d, read rank_0 content = %d\n", my_rank, recv);
    //MPI_Win_fence(0, my_win);//sync close

    //MPI_Win_fence(0, my_win);
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, my_rank, 0, my_win);//MPI_LOCK_SHARED
    MPI_Get(&buf_get, 2 * MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, my_rank, 0, 2 * MAIL_BAG_SIZE * MSG_SIZE_MAX, MPI_CHAR, my_win);
    MPI_Win_unlock(my_rank, my_win);
    //MPI_Win_fence(0, my_win);
    for(int i = 0; i < 2 * MAIL_BAG_SIZE; i++){
        //printf("%s\n", my_bag.bag[i].message);
        printf("Print buf_get at rank %d: %s\n", my_rank, buf_get[i]);
    }

    //rotate mailbags:



    //verify rank, window and content


    //lock ops: read, add, read.



    free(my_bag.bag);
    MPI_Win_free(&my_win);

    MPI_Finalize();
    return 0;
}
