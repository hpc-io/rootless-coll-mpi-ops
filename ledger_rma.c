#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char** argv) {
    // Initialize the MPI environment
    MPI_Init(NULL, NULL);

    // Get the number of processes

    /* create private memory */
    int* shared_int;
    int rank_shared = 0;
    /* collectively declare memory as remotely accessible */
    MPI_Win my_win;
    MPI_Win_allocate(1 * sizeof(int), sizeof(int), MPI_INFO_NULL, MPI_COMM_WORLD, &shared_int, &my_win);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    // Get the rank of the process
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    // Get the name of the processor
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(processor_name, &name_len);

    // Print off a hello world message
    printf("Hello world from processor %s, rank %d out of %d processors\n",
            processor_name, my_rank, world_size);
    char win_name_set[MPI_MAX_PROCESSOR_NAME];
    char win_name_get[MPI_MAX_PROCESSOR_NAME];

    sprintf(win_name_set, "win_%d", my_rank);

    MPI_Win_set_name(my_win, win_name_set);
    int resultlen;
    MPI_Win_get_name(my_win, win_name_get, &resultlen);
    //initialize a var in local window, set to rank.
    MPI_Win_fence(0, my_win);//collective sync open
    MPI_Put(&my_rank, 1, MPI_INT, my_rank, 0 ,1, MPI_INT, my_win);
    MPI_Win_fence(0, my_win);//sync close

    int recv = 0;
    //MPI_Win_fence(0, my_win);//collective sync open
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, rank_shared, 0, my_win);//MPI_LOCK_SHARED
    MPI_Get(&recv, 1, MPI_INT, rank_shared, 0, 1, MPI_INT, my_win);
    printf("my rank = %d, read rank_0 content = %d\n", my_rank, recv);
    MPI_Put(&my_rank, 1, MPI_INT, rank_shared, 0 ,1, MPI_INT, my_win);
//    MPI_Win_fence(0, my_win);//sync close
//
//    MPI_Win_fence(0, my_win);//sync open
    MPI_Get(&recv, 1, MPI_INT, rank_shared, 0, 1, MPI_INT, my_win);
    printf("After put: my rank = %d, read rank_0 content = %d\n", my_rank, recv);
    //MPI_Win_fence(0, my_win);//sync close
    MPI_Win_unlock(rank_shared, my_win);




    //verify rank, window and content


    //lock ops: read, add, read.





    MPI_Win_free(&my_win);

    MPI_Finalize();
    return 0;
}
