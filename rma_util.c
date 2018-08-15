/*
 * rma_util.c
 *
 *  Created on: Aug 13, 2018
 *      Author: tonglin
 */

//tested
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


