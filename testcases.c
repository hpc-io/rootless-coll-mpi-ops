/*
 * testcases.c
 *
 *  Created on: Jul 17, 2019
 *      Author: tonglin
 */

#include "rootless_ops.h"
#define DEBUG_PRINT printf("%s:%u, pid = %d\n", __func__, __LINE__, getpid());
//Toy callback functions for proposal judging and approval.
int is_proposal_approved_cb(const void *buf, void *app_data);
int proposal_action_cb(const void *buf, void *app_data);
int aggregate_test_result(MPI_Comm comm, int test_passed, char* test_name);

int test_IAllReduce_single_proposal(MPI_Comm comm, int starter, int no_rank, int agree);
int test_concurrent_iar_multi_proposal(MPI_Comm comm, int active_1, int active_2_mod, int agree);

int is_proposal_approved_cb(const void *proposal, void *_app_ctx){
    ISP* my_isp = (ISP*)_app_ctx;

    if(!my_isp->my_proposal || strlen(my_isp->my_proposal) == 0)//I don't have a proposal, so I approve anything coming in.
        return 1;

    if(!proposal)
        return 1;

    if(strcmp(my_isp->my_proposal, proposal) == 0){//compatible
        printf("%s:%u return 1\n", __func__, __LINE__);
        return 1;
    }

    if(((char*)proposal)[0] < ((char*)my_isp->my_proposal)[0]) {
        printf("%s:%u return 0\n", __func__, __LINE__);
        return 0;
    }else
        return 1;
}

int proposal_action_cb(const void *buf, void *app_data){
//    printf("          Callback function: Proposal action started.\n");
    return 0;
}

typedef struct DS_TMP{
    iar_cb_func_t my_func;
}ds_tmp;

int test_callback(){
    ds_tmp* my_ds = calloc(1, sizeof(ds_tmp));
    my_ds->my_func = &is_proposal_approved_cb;
    //iar_cb_func_t my_func = &is_proposal_approved_cb;
    char* buf = "<<This is a buf>>";
    char* ctx = "<<This is a contex.>>";
    printf("Callback caller output: returns %d\n", (*(my_ds->my_func))(buf, ctx));
    //_test_caller((is_proposal_approved_cb)(void));
    return 0;
}

int test_gen_bcast(int buf_size, int root_rank, int cnt){
    int my_rank = RLO_get_my_rank();
    int world_size = RLO_get_world_size();
    int test_passed = 0;
    assert(root_rank < world_size);
    if(buf_size > RLO_MSG_SIZE_MAX) {
        printf("Message size too big. Maximum allowed is %d\n", RLO_MSG_SIZE_MAX);
        return -1;
    }
    RLO_engine_t* eng = RLO_progress_engine_new(MPI_COMM_WORLD, RLO_MSG_SIZE_MAX, NULL, NULL, NULL);

    int recved_cnt = 0;
    unsigned long start = RLO_get_time_usec();
    unsigned long time_send = 0;
    RLO_msg_t* recv_msg = NULL;

    if(my_rank == root_rank) {//send
        //load data for bcast
        char buf[64] = "";
        for(int i = 0; i < cnt; i++) {
            sprintf(buf, "Pretty_Long_Message_from_rank_%d_No.%d", my_rank, i);
            RLO_msg_t* send_msg = RLO_msg_new_bc(eng, buf, strlen(buf) + 1);
            RLO_bcast_gen(eng, send_msg, RLO_BCAST);
            RLO_make_progress();
            recv_msg = NULL;
        }
    } else {//recv
        do{
            recv_msg = NULL;
            RLO_make_progress();
            RLO_user_msg* pickup_out = NULL;
            while(RLO_user_pickup_next(eng, &pickup_out)){
                recved_cnt++;
                char* str_start = (char*)(pickup_out->data) + sizeof(size_t);
                printf("Received msg = [%s]\n", str_start);
                RLO_user_msg_recycle(eng, pickup_out);//free it here if(fwd_done)
            }
        } while(recved_cnt < cnt);
    }
    unsigned long end = RLO_get_time_usec();

    time_send = end - start;

    if(my_rank != root_rank){
        printf("Rank %d: Received %d times, cnt = %d\n", my_rank, recved_cnt, cnt);
        test_passed = (recved_cnt == cnt);
    }else{
        test_passed = (recved_cnt == 0);
    }
    RLO_progress_engine_cleanup(eng);
    return test_passed;
}

int test_concurrent_iar_single_proposal(MPI_Comm comm, int starter, int no_rank, int agree) {
    ISP isp;
    isp.my_proposal = NULL;
    int my_rank = RLO_get_my_rank();
    int world_size = RLO_get_world_size();
    assert((starter < world_size) && (no_rank < world_size));
    RLO_engine_t* eng = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);
    RLO_engine_t* eng2 = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);
    char* my_proposal = "111";
    char* proposal_2 = "333";

    int result, result2;
    int pass, pass2;

    if (my_rank == starter) {
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        isp.my_proposal = my_proposal;
        int my_proposal_id = my_rank;
         int ret = RLO_submit_proposal(eng, my_proposal, strlen(my_proposal) + 1, my_proposal_id);
         int ret2 = RLO_submit_proposal(eng2, my_proposal, strlen(my_proposal) + 1, my_proposal_id);

         if(ret > -1 && ret2 > -1){//done
             result = RLO_get_vote_my_proposal(eng);
             result2 = RLO_get_vote_my_proposal(eng2);
             printf("\n Both engines got results for proposal: %s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
         }else{//sample application logic loop
             RLO_Req_stat s1 = RLO_check_my_proposal_state(eng, starter);
             RLO_Req_stat s2 = RLO_check_my_proposal_state(eng2, starter);
             while(s1 != RLO_COMPLETED || s2!= RLO_COMPLETED){
                 RLO_make_progress();
                 s1 = RLO_check_my_proposal_state(eng, starter);
                 s2 = RLO_check_my_proposal_state(eng2, starter);
                 printf("\n %s:%u - rank = %03d: check proposal: s1 = %d, s2 = %d\n", __func__, __LINE__, my_rank, s1, s2);
             }
//             while(RLO_check_my_proposal_state(eng, starter) != RLO_COMPLETED || RLO_check_my_proposal_state(eng2, starter) != RLO_COMPLETED){
//                 //make_progress_all();
//                 RLO_make_progress();
//             }
             result = RLO_get_vote_my_proposal(eng);
             result2 = RLO_get_vote_my_proposal(eng2);
         }

         //if(RLO_check_my_proposal_state(eng, 0) == RLO_COMPLETED)
             printf("E1: %s:%u - rank = %03d: check proposal: result1 = %d, state = %d\n", __func__, __LINE__, my_rank, result, RLO_check_my_proposal_state(eng, starter));
         //if(RLO_check_my_proposal_state(eng2, 0) == RLO_COMPLETED)
             printf("E2: %s:%u - rank = %03d: check proposal: result2 = %d, state = %d\n", __func__, __LINE__, my_rank, result2, RLO_check_my_proposal_state(eng2, starter));

        pass = (result == agree);
        pass2 = (result2 == agree);

    } else {//passive ranks
        usleep(200);
        if (my_rank  == no_rank) {// Passive opponent
            if(agree){
                isp.my_proposal = my_proposal;
            } else {
                isp.my_proposal = proposal_2;
            }
        } else {
          isp.my_proposal = "";
        }
        int tag_recv = -1;
        int tag_recv2 = -1;
        int decision_cnt = 0;
        do {
            RLO_make_progress();
            RLO_user_msg* pickup_out = NULL;
            while(RLO_user_pickup_next(eng, &pickup_out)){
                //printf("%s:%u - rank = %03d: pickup_out = %p\n", __func__, __LINE__, my_rank, pickup_out);
                //PBuf* pbuf = malloc(sizeof(PBuf));
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
                //pbuf_deserialize(pickup_out->data_buf, pbuf);

                tag_recv = pickup_out->type;
                switch (tag_recv) {//pickup_out->irecv_stat.MPI_TAG
                    case RLO_IAR_PROPOSAL: //2
                        printf("E1: Rank %d: Received proposal: %d:%s.\n", my_rank, pickup_out->pid, pickup_out->data);
                        break;
                    case RLO_IAR_VOTE: //3
                        printf("E1: Rank %d: Received vote: %d:%d.\n", my_rank, pickup_out->pid, pickup_out->vote);
                        break;
                    case RLO_IAR_DECISION: //4
                        decision_cnt++;
                        printf("E1: Rank %d: Engine %d: Received decision: %d:%d\n", my_rank, RLO_get_engine_id(eng), pickup_out->pid, pickup_out->vote);
                        pass = (pickup_out->vote == agree);
                        break;

                    default:
                        printf("E1: Warning: Rank %d: Received unexpected msg, tag = %d.\n", my_rank, tag_recv);
                        break;
                }
                RLO_user_msg_recycle(eng, pickup_out);
                pickup_out = NULL;
            }

            RLO_make_progress();
            RLO_user_msg* pickup_out2 = NULL;
            while(RLO_user_pickup_next(eng2, &pickup_out2)){
                //printf("%s:%u - rank = %03d: pickup_out = %p\n", __func__, __LINE__, my_rank, pickup_out);
                //PBuf* pbuf = malloc(sizeof(PBuf));
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
                //pbuf_deserialize(pickup_out->data_buf, pbuf);

                tag_recv2 = pickup_out2->type;
                switch (tag_recv2) {//pickup_out->irecv_stat.MPI_TAG
                    case RLO_IAR_PROPOSAL: //2
                        printf("E2: Rank %d: Received proposal: %d:%s.\n", my_rank, pickup_out2->pid, pickup_out2->data);
                        break;
                    case RLO_IAR_VOTE: //3
                        printf("E2: Rank %d: Received vote: %d:%d.\n", my_rank, pickup_out2->pid, pickup_out2->vote);
                        break;
                    case RLO_IAR_DECISION: //4
                        decision_cnt++;
                        printf("E2: Rank %d: Engine %d: Received decision: %d:%d\n", my_rank, RLO_get_engine_id(eng2), pickup_out2->pid, pickup_out2->vote);
                        pass2 = (pickup_out2->vote == agree);
                        break;

                    default:
                        printf("E2: Warning: Rank %d: Received unexpected msg, tag = %d.\n", my_rank, tag_recv2);
                        break;
                }
                RLO_user_msg_recycle(eng2, pickup_out2);
                pickup_out2 = NULL;
            }
        } while (tag_recv != RLO_IAR_DECISION && tag_recv2 != RLO_IAR_DECISION);
    }

    if(my_rank == starter) {
        printf("\n\n Starter: rank %d: =========== decision: Proposal result1 = %d, result2 = %d =========== \n\n", starter, result, result2);
    }

    MPI_Comm my_comm = RLO_get_my_comm(eng);
    MPI_Comm my_comm2 = RLO_get_my_comm(eng2);

    RLO_progress_engine_cleanup(eng);
    RLO_progress_engine_cleanup(eng2);

    MPI_Barrier(my_comm);
    MPI_Barrier(my_comm2);

    int r = aggregate_test_result(my_comm, pass, "Eng-1: Single proposal IAllReduce");
    int r2 = aggregate_test_result(my_comm2, pass2, "Eng-2: Single proposal IAllReduce");
    return (r == 1 && r2 == 1);
}
//no_rank: the rank that votes "NO".
int test_IAllReduce_single_proposal(MPI_Comm comm, int starter, int no_rank, int agree) {
    ISP isp;
    isp.my_proposal = NULL;
    int my_rank = RLO_get_my_rank();
    int world_size = RLO_get_world_size();
    assert((starter < world_size) && (no_rank < world_size));
    RLO_engine_t* eng = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);
    char* my_proposal = "111";
    char* proposal_2 = "333";

    int result = -1;
    int ret = -1;
    int pass = 0;

    if (my_rank == starter) {
        printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
        isp.my_proposal = my_proposal;
        int my_proposal_id = my_rank;
         ret = RLO_submit_proposal(eng, my_proposal, strlen(my_proposal), my_proposal_id);
         if(ret > -1){//done
             result = RLO_get_vote_my_proposal(eng);
             printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
         }else{//sample application logic loop
             while(RLO_check_my_proposal_state(eng, 0) != RLO_COMPLETED){
                 //make_progress_all();
                 RLO_make_progress();
             }
             result = RLO_get_vote_my_proposal(eng);
         }
        printf("%s:%u - rank = %03d: proposal completed, decision = %d \n", __func__, __LINE__, my_rank, result);
        pass = (result == agree);

    } else {//passive ranks
        usleep(200);
        if (my_rank  == no_rank) {// Passive opponent
            if(agree){
                isp.my_proposal = my_proposal;
            } else {
                isp.my_proposal = proposal_2;
            }
        } else {
          isp.my_proposal = "";
          //printf("strlen(my_proposal) = %lu\n", strlen(isp.my_proposal));
        }
        int tag_recv = -1;
        do {
            RLO_make_progress();
            RLO_user_msg* pickup_out = NULL;
            usleep(300);
            while(RLO_user_pickup_next(eng, &pickup_out)){
                //printf("%s:%u - rank = %03d: pickup_out = %p\n", __func__, __LINE__, my_rank, pickup_out);
                //PBuf* pbuf = malloc(sizeof(PBuf));
                //printf("%s:%u - rank = %03d\n", __func__, __LINE__, my_rank);
                //pbuf_deserialize(pickup_out->data_buf, pbuf);

                tag_recv = pickup_out->type;
                switch (tag_recv) {//pickup_out->irecv_stat.MPI_TAG
                    case RLO_IAR_PROPOSAL: //2
                        printf("Rank %d: Received proposal: %d:[%s].\n", my_rank, pickup_out->pid, pickup_out->data);
                        break;
                    case RLO_IAR_VOTE: //3
                        printf("Rank %d: Received vote: %d:%d.\n", my_rank, pickup_out->pid, pickup_out->vote);
                        break;
                    case RLO_IAR_DECISION: //4
                        printf("Rank %d: Received decision: %d:%d\n", my_rank, pickup_out->pid, pickup_out->vote);
                        pass = (pickup_out->vote == agree);
                        break;

                    default:
                        printf("Warning: Rank %d: Received unexpected msg, tag = %d.\n", my_rank, tag_recv);
                        break;
                }
                RLO_user_msg_recycle(eng, pickup_out);
                pickup_out = NULL;

            }
        } while (tag_recv != RLO_IAR_DECISION);
    }

    if(my_rank == starter) {
        if (result) {
            printf("\n\n Starter: rank %d: =========== decision: Proposal approved =========== \n\n", starter);
        } else {
            printf("\n\n Starter: rank %d: =========== decision: Proposal declined =========== \n\n", starter);
        }
    }

    MPI_Comm my_comm = RLO_get_my_comm(eng);
    RLO_progress_engine_cleanup(eng);
    return aggregate_test_result(my_comm, pass, "Single proposal IAllReduce");
}

//Basic testcase for multiple mpi communicator
int testcase_iar_single_multiComm(){
    test_IAllReduce_single_proposal(MPI_COMM_WORLD, 1, 2, 0);
    test_IAllReduce_single_proposal(MPI_COMM_WORLD, 1, 2, 1);
    return 0;
}

int testcase_iar_concurrent_single_proposal(){
    int proposer = 1;
    int opponent = 2;//says no
    test_concurrent_iar_single_proposal(MPI_COMM_WORLD, proposer, opponent, 0);
    test_concurrent_iar_single_proposal(MPI_COMM_WORLD, proposer, opponent, 1);
    return 0;
}

int testcase_iar_concurrent_multiple_proposal(){
    test_concurrent_iar_multi_proposal(MPI_COMM_WORLD, 1, 3, 1);
    if(RLO_get_my_rank() == 0)
        printf("\n\n 2nd test started... \n\n");
    test_concurrent_iar_multi_proposal(MPI_COMM_WORLD, 1, 3, 0);
    return 0;
}
//Test case utility, used ONLY by a rank that has submitted a proposal.
int util_testcase_decision_receiver(RLO_engine_t* eng, int decision_needed){
    RLO_make_progress();
    int my_rank = RLO_get_my_rank();
    int cnt_yes = 0;
    int cnt_no = 0;
    RLO_user_msg* pickup_out = NULL;
    int decision_cnt = 0;
    int tag_recv = -1;
    while (decision_cnt < decision_needed) {
        RLO_make_progress();
        while (RLO_user_pickup_next(eng, &pickup_out)) {
            RLO_make_progress();
            //printf("%s:%u - rank = %03d: pickup_out = %p\n", __func__, __LINE__, my_rank, pickup_out);
            //PBuf* pbuf = malloc(sizeof(PBuf));
            //pbuf_deserialize(pickup_out->data_buf, pbuf);

            tag_recv = pickup_out->type;
            switch (tag_recv) { //pickup_out->irecv_stat.MPI_TAG
                case RLO_IAR_PROPOSAL: //2
                    printf("Rank %d: Received proposal: %d:%s.\n", my_rank, pickup_out->pid,
                            pickup_out->data);
                    break;
                case RLO_IAR_VOTE: //3
                    printf("Rank %d: Received vote: %d:%d.\n", my_rank, pickup_out->pid, pickup_out->vote);
                    break;
                case RLO_IAR_DECISION: //4
                    decision_cnt++;
                    printf("Rank %d: Received decision: %d:%d, decision_cnt = %d, needed %d, data = [%s]\n",my_rank, pickup_out->pid,
                            pickup_out->vote, decision_cnt, decision_needed, pickup_out->data);
                    if(pickup_out->vote)
                        cnt_yes++;
                    else
                        cnt_no++;

                    break;

                default:
                    printf("Warning: Rank %d: Received unexpected msg, tag = %d.\n", my_rank,
                            tag_recv);
                    break;
            }
            RLO_user_msg_recycle(eng, pickup_out);
            pickup_out = NULL;
        }
    }
    return cnt_yes + cnt_no;
}

int test_iar_multi_proposal(MPI_Comm comm, int active_1, int active_2_mod, int agree) {
    char* my_proposal = "555";
    ISP isp;
    isp.my_proposal = NULL;
    int world_size = RLO_get_world_size();
    int pass = 0;

    assert((active_1 < world_size) && (active_2_mod < world_size));
    RLO_engine_t* eng = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);

    int result = -1;
    int ret = -1;
    //Rank 0, first active, mod active rank
    int decision_needed = 1 + (world_size - 1) / active_2_mod + 1;
    int my_rank = RLO_get_my_rank();

    if (my_rank == active_1) {
        printf(" \n-------- First active rank %d--------\n\n", my_rank);
        printf("\n%s:%u - rank = %03d:  world_size = %d, mod = %d, %d decisions are needed. \n\n",
                __func__, __LINE__, my_rank, world_size, active_2_mod, decision_needed);

        isp.my_proposal = my_proposal;

        int my_proposal_id = my_rank;

         ret = RLO_submit_proposal(eng, my_proposal, strlen(my_proposal), my_proposal_id);
         if(ret > -1){//done
             result = RLO_get_vote_my_proposal(eng);
         }else{//sample application logic loop
             while(RLO_check_my_proposal_state(eng, 0) != RLO_COMPLETED){
                 RLO_make_progress();
             }
             result = RLO_get_vote_my_proposal(eng);

             RLO_make_progress();
             pass = (util_testcase_decision_receiver(eng, decision_needed - 1) == decision_needed - 1);
         }
    } else if (my_rank % active_2_mod == 0) {
        printf(" \n-------- Mod active rank %d--------\n\n", my_rank);

        int my_proposal_id = my_rank;

        char* proposal_2 = "333";

        if(agree){
            isp.my_proposal = my_proposal;
        } else {
            isp.my_proposal = proposal_2;
        }

        ret = RLO_submit_proposal(eng, isp.my_proposal, strlen(isp.my_proposal), my_proposal_id);
        if (ret > -1) { //done
            result = RLO_get_vote_my_proposal(eng);
        } else { //sample application logic loop
            while (RLO_check_my_proposal_state(eng, 0) != RLO_COMPLETED) {
                RLO_make_progress();
            }
            result = RLO_get_vote_my_proposal(eng);

            RLO_make_progress();
            pass = (util_testcase_decision_receiver(eng, decision_needed - 1) == decision_needed - 1);
        }//eld else: ret <= -1.
    } else {//all passive ranks
        if(agree){
            isp.my_proposal = my_proposal;
        } else {
            isp.my_proposal = "111";
        }

        RLO_make_progress();
        pass = (util_testcase_decision_receiver(eng, decision_needed) == decision_needed);
    }

    MPI_Comm my_comm = RLO_get_my_comm(eng);
    RLO_progress_engine_cleanup(eng);

    if(my_rank == active_1 || my_rank == active_2_mod) {
        if (result) {
            printf("\n\n Active rank %d: =========== decision: Proposal approved =========== \n\n", my_rank);
        } else {
            printf("\n\n Active rank %d: =========== decision: Proposal declined =========== \n\n", my_rank);
        }
    }

    return aggregate_test_result(my_comm, pass, "Multi-proposal IAllReduce");
}

int test_concurrent_iar_multi_proposal(MPI_Comm comm, int active_1, int active_2_mod, int agree) {
    char* my_proposal = "555";
    ISP isp;
    isp.my_proposal = NULL;
    int world_size = RLO_get_world_size();
    int pass, pass2;

    assert((active_1 < world_size) && (active_2_mod < world_size));
    RLO_engine_t* eng = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);
    RLO_engine_t* eng2 = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, &is_proposal_approved_cb, &isp, &proposal_action_cb);

    int result, result2;
    int ret, ret2;
    //Rank 0, first active, mod active rank
    int decision_needed = 1 + (world_size - 1) / active_2_mod + 1;
    int my_rank = RLO_get_my_rank();

    if (my_rank == active_1) {
        printf(" \n-------- First active rank %d--------\n\n", my_rank);
        printf("\n%s:%u - rank = %03d:  world_size = %d, mod = %d, %d decisions are needed. \n\n",
                __func__, __LINE__, my_rank, world_size, active_2_mod, decision_needed);

        isp.my_proposal = my_proposal;

        int my_proposal_id = my_rank;

         ret = RLO_submit_proposal(eng, my_proposal, strlen(my_proposal), my_proposal_id);
         ret2 = RLO_submit_proposal(eng2, my_proposal, strlen(my_proposal), my_proposal_id);
         if(ret > -1 && ret2 > -1){//done
             result = RLO_get_vote_my_proposal(eng);
             result2 = RLO_get_vote_my_proposal(eng2);
         }else{//sample application logic loop
             while(RLO_check_my_proposal_state(eng, 0) != RLO_COMPLETED || RLO_check_my_proposal_state(eng2, 0) != RLO_COMPLETED){
                 RLO_make_progress();
             }
             result = RLO_get_vote_my_proposal(eng);
             result2 = RLO_get_vote_my_proposal(eng2);

             RLO_make_progress();
             pass = (util_testcase_decision_receiver(eng, decision_needed - 1) == decision_needed - 1);
             pass2 = (util_testcase_decision_receiver(eng2, decision_needed - 1) == decision_needed - 1);
         }
    } else if (my_rank % active_2_mod == 0) {
        printf(" \n-------- Mod active rank %d--------\n\n", my_rank);

        int my_proposal_id = my_rank;

        char* proposal_2 = "333";

        if(agree){
            isp.my_proposal = my_proposal;
        } else {
            isp.my_proposal = proposal_2;
        }

        ret = RLO_submit_proposal(eng, isp.my_proposal, strlen(isp.my_proposal), my_proposal_id);
        ret2 = RLO_submit_proposal(eng2, isp.my_proposal, strlen(isp.my_proposal), my_proposal_id);

        if(ret > -1 && ret2 > -1){//done
            result = RLO_get_vote_my_proposal(eng);
            result2 = RLO_get_vote_my_proposal(eng2);
        }else{//sample application logic loop
            while(RLO_check_my_proposal_state(eng, 0) != RLO_COMPLETED || RLO_check_my_proposal_state(eng2, 0) != RLO_COMPLETED){
                RLO_make_progress();
            }
            result = RLO_get_vote_my_proposal(eng);
            result2 = RLO_get_vote_my_proposal(eng2);

            RLO_make_progress();
            pass = (util_testcase_decision_receiver(eng, decision_needed - 1) == decision_needed - 1);
            pass2 = (util_testcase_decision_receiver(eng2, decision_needed - 1) == decision_needed - 1);
        }

    } else {//all passive ranks
        if(agree){
            isp.my_proposal = my_proposal;
        } else {
            isp.my_proposal = "111";
        }

        RLO_make_progress();
        pass = (util_testcase_decision_receiver(eng, decision_needed) == decision_needed);
        pass2 = (util_testcase_decision_receiver(eng2, decision_needed) == decision_needed);
    }

    MPI_Comm my_comm = RLO_get_my_comm(eng);
    MPI_Comm my_comm2 = RLO_get_my_comm(eng2);

    RLO_progress_engine_cleanup(eng);
    RLO_progress_engine_cleanup(eng2);

    if(my_rank == active_1 || my_rank == active_2_mod) {
        if (result) {
            printf("\n\n Active rank %d: =========== decision: Proposal approved =========== \n\n", my_rank);
        } else {
            printf("\n\n Active rank %d: =========== decision: Proposal declined =========== \n\n", my_rank);
        }
    }

    MPI_Barrier(my_comm);
    MPI_Barrier(my_comm2);

    int r1 = aggregate_test_result(my_comm ,pass, "Eng-1: Multi-proposal IAllReduce");
    int r2 = aggregate_test_result(my_comm2 ,pass2, "Eng-2: Multi-proposal IAllReduce");

    return(r1 == 1 && r2 == 1);
}


int get_prev_rank(int my_rank, int world_size) {
    return (my_rank + (world_size - 1)) % world_size;
}

int get_next_rank(int my_rank, int world_size) {
    return (my_rank + 1) % world_size;
}

int get_random_rank(int my_rank, int world_size) {
    int next_rank;

    do {
        next_rank = rand() % world_size;
    } while(next_rank == my_rank);

    return next_rank;
}

int aggregate_test_result(MPI_Comm comm, int test_passed, char* test_name) {
    int total_pass = 0;
    int my_rank = RLO_get_my_rank();
    int world_size = RLO_get_world_size();
    int ret = -1;
    MPI_Reduce(&test_passed, &total_pass, 1, MPI_INT, MPI_SUM, 0, comm);

    if (total_pass == world_size) {
        if (my_rank == 0) {
            printf("\n\n%s test passed. total_pass = %d\n\n", test_name, total_pass);
        }
        ret = 1;
    } else {
        if (my_rank == 0) {
            printf("\n\n%s test failed: %d ranks succeeded, %d expected. total_pass = %d\n\n", test_name, total_pickup,
                    world_size, total_pass);
        }
        ret = 0;
    }

    return ret;
}

int hacky_sack_progress_engine(MPI_Comm comm, int msg_cnt){
    int my_rank = RLO_get_my_rank();

    RLO_engine_t* eng = RLO_progress_engine_new(comm, RLO_MSG_SIZE_MAX, NULL, NULL, NULL);

    unsigned long time_start;
    unsigned long time_end;
    unsigned long phase_1;
    int bcast_sent_cnt;
    int recv_msg_cnt = 0;
    //char buf_send[32] = "";
    int buf_size = 32;
    char* buf_send = calloc(1, buf_size);

    /* Get pointer to sending buffer */
    total_pickup = 0;
    time_start = RLO_get_time_usec();
    int world_size = RLO_get_world_size();
    /* Compose message to send (in bcomm's send buffer) */
    int next_rank = get_next_rank(my_rank, world_size);//get_random_rank(my_rank, world_size);;
            //get_random_rank(my_rank, world_size);
            //get_next_rank(my_rank, world_size);
            //get_prev_rank(my_rank, world_size);
    sprintf(buf_send, "%d", next_rank);

    /* Broadcast message */
    RLO_msg_t* prev_rank_bc_msg = RLO_msg_new_bc(eng, buf_send, strlen(buf_send) + 1);
    RLO_bcast_gen(eng, prev_rank_bc_msg, RLO_BCAST);
    usleep(100);
    bcast_sent_cnt = 0;
    RLO_user_msg* pickup_out = NULL;
    //MPI_Barrier(MPI_COMM_WORLD);
    while (bcast_sent_cnt < msg_cnt) {
        RLO_make_progress();

        while(RLO_user_pickup_next(eng, &pickup_out)){
            assert(pickup_out);
            printf("Hacky sacking: received msg = [%s]\n", pickup_out->data+sizeof(size_t));
            recv_msg_cnt++;
            total_pickup++;
            int recv_rank = atoi((pickup_out->data+sizeof(size_t)));
            RLO_user_msg_recycle(eng, pickup_out);
            if(recv_rank == my_rank){
                //MPI_Barrier(MPI_COMM_WORLD);
                char buf[32] = "";
                next_rank = get_next_rank(my_rank, world_size);
                sprintf(buf, "%d", next_rank);
                RLO_msg_t* next_msg_send = RLO_msg_new_bc(eng, buf, strlen(buf) + 1);
                RLO_bcast_gen(eng, next_msg_send, RLO_BCAST);
                bcast_sent_cnt++;
            }
        }
    }

    time_end = RLO_get_time_usec();
    phase_1 = time_end - time_start;
    RLO_progress_engine_cleanup(eng);
    MPI_Barrier(MPI_COMM_WORLD);
    int expected_pickup = (bcast_sent_cnt + 1) * (world_size - 1);
    unsigned int pass = (total_pickup == expected_pickup);
    printf("Rank %02d reports: Hacky sack done passive bcast %d times. total pickup %d times. expected pickup = %d, pass = %d\n", my_rank,
            bcast_sent_cnt,  total_pickup, expected_pickup, pass);
    free(buf_send);
    return pass;
}

int test_wrapper_bcast(int bc_cnt){
    int failed = 0;
    int my_rank = RLO_get_my_rank();
    int world_size = RLO_get_world_size();
    int test_passed = 0;
    for(int i = 0; i < world_size; i++){
//        MPI_Barrier(MPI_COMM_WORLD);
        test_passed = test_gen_bcast(RLO_MSG_SIZE_MAX, i, bc_cnt);
        if(test_passed != 1)
            failed++;
    }

    int total_failed = 0;

    MPI_Reduce(&failed, &total_failed, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    if(my_rank == 0){
        int pass = (total_failed == 0);
        if(pass)
            printf("\n\nBcast tests passed. \n\n");
        else
            printf("\n\nBcast tests failed, %d test case failed\n\n", failed);
        return pass;
    }
    return (total_failed == 0);
}

int test_wrapper_hackysacking(int cnt_round, int cnt_msg){
    int failed = 0;
    //int my_rank = get_my_rank();
    MPI_Comm comm;
    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
    for (int i = 0; i < cnt_round; i++) {
        int succ = hacky_sack_progress_engine(comm, cnt_msg);
        if(succ != 1){
            failed++;
            break;
        }
    }
    int pass = (failed == 0);
    return aggregate_test_result(comm, pass, "Hackysack");
}

int pbuf_test(){
    void* proposal_send_buf = NULL;//calloc(1, RLO_MSG_SIZE_MAX);
    size_t buf_len;
    RLO_time_stamp time = RLO_get_time_usec();
    char* proposal = "test proposal";
    int my_proposal_id = 10;
    if(0 != pbuf_serialize(my_proposal_id, 1, time, strlen(proposal)+1, (void*)proposal, &proposal_send_buf, &buf_len)) {
        printf("pbuf_serialize failed.\n");
        return -1;
    }

    PBuf* s = pbuf_new_local(my_proposal_id, 1, time, strlen(proposal)+1, (void*)proposal);

    pbuf_debug(s, proposal_send_buf);
    DEBUG_PRINT
    PBuf* tmp = NULL;
    pbuf_deserialize(proposal_send_buf , &tmp);
    printf("%s: - %d: Verifying pbuf_deserialize(): tmp pid = %d, should be %d, data_len = %lu, should be %lu\n",
            __func__, __LINE__, tmp->pid, my_proposal_id, tmp->data_len, strlen(proposal));
    pbuf_free(tmp);
    free(proposal_send_buf);
    return 0;
}

int main(int argc, char** argv) {
    time_t t;
    srand((unsigned) time(&t) + getpid());

    MPI_Init(NULL, NULL);

    int my_rank = RLO_get_my_rank();

    printf("%s:%u - rank = %03d, pid = %d\n", __func__, __LINE__, my_rank, getpid());

    if(argc > 1){
        int sleep_time = atoi(argv[1]);
        printf("Waiting %d sec for gdb to attach....pid = %d\n", sleep_time, getpid());
        sleep(sleep_time);
    }

   //sleep(30);
    // ======================== Basic bcast test ========================
    //test_gen_bcast(RLO_MSG_SIZE_MAX, 1, 1);

    // ======================== All-to-all complex bcast test: Hackysacking ========================
    //test_wrapper_hackysacking(3, 1);

    // ======================== IAll_Reduce tests ========================

    testcase_iar_single_multiComm();
    //pbuf_test();
    //testcase_iar_concurrent_single_proposal();
    //*testcase_iar_concurrent_multiple_proposal();
    //test_concurrent_iar_multi_proposal(MPI_COMM_WORLD, 1, 3, 1);

//    test_wrapper_bcast(2);
////
//    int init_rank = atoi(argv[1]);
//    int no_rank = atoi(argv[2]); //Rank that says 'NO'
//    int agree = atoi(argv[3]);
//    MPI_Comm comm;
//    MPI_Comm_dup(MPI_COMM_WORLD, &comm);
    //sleep(30);
//    test_IAllReduce_single_proposal(my_bcomm, init_rank, no_rank, agree);

    //test_concurrent_iar_multi_proposal(comm, init_rank, no_rank, agree);

    MPI_Finalize();

    return 0;
}
