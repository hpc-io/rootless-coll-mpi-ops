///*
// * progress_engine.h
// *
// *  Created on: Nov 28, 2018
// *      Author: tonglin
// */
//
//#ifndef PROGRESS_ENGINE_H_
//#define PROGRESS_ENGINE_H_
//
//typedef struct Msg_state {
//  //Kind_t kind;   // ‘a’, ‘b’. ‘c’, etc.
//    union {
//        enum State_BC bc_state;
//        enum State_IAR iar_state;
//
//    } state;
//} Msg_state;
//
//typedef struct bcomm_BC_msg {
//    char buf[BUF_MAX];  // Make this always be the first field, so a pointer to it is the same as a pointer to the message struct
//    Msg_state stat;
//    MPI_Request req;
//    bcomm_BC_msg_t *next_forwarding, *prev_forwarding;//still in forwarding
//    bcomm_BC_msg_t *next_app_recv, *prev_app_recv;//ready for user to look at the buffer
//}bcomm_BC_msg_t;
//
//typedef struct bcomm_IAR_msg {
//    char buf[BUF_MAX];    // Make this always be the first field, so a pointer to it is the same as a pointer to the message struct
//    ID proposal_id;
//    Msg_state stat;
//    MPI_Request req;
//
//    bcomm_BC_msg_t *next, *prev;
//}bcomm_IAR_msg_t;
//
//
//struct bcomm_t_new {
//    bcomm_BC_msg_t *new_bcast; //post irecv with tag = bcast
//    bcomm_IAR_msg_t *new_IAR_prop; //post irecv with tag = IAR
//    bcomm_BC_msg_t // received msg for forwarding -- infra
//            *bcast_msg_forwarding_queue_head,
//            *bcast_msg_forwarding_queue_tail;
//    bcomm_BC_msg_t // received msg for app to read.
//            *bcast_msg_app_recv_queue_head,
//            *bcast_msg_app_recv_queue_tail;
//    bcomm_IAR_msg_t // -- for infra use
//            *IAR_msg_queue_head,
//            *IAR_msg_queue_tail;
//    bcomm_IAR_msg_t // for app use, probably don’t need this queue, if we make callbacks to user function pointer
//            *IAR_msg_app_queue_head,
//            *IAR_msg_app_queue_tail;
//};
//
//enum State_BC {
//    NEW, // I_recv not completed yet
//    NOT_APP_RECV_STILL_FORWARDING, // need appl to look at buffer, still forwarding to children
//    APP_RECV_STILL_FORWARDING,  //app has recevied, still forwarding to children
//    NOT_APP_RECV_DONE_FORWARDING, // need appl to look at buffer, done forwardiing to children
//    APP_RECV_DONE_FORWARDING,  //app has recevied, done forwarding to children
//    APP_RECV_AND_FINISHED_STILL_FORWARDING,
//    // appl finished looking at buffer, still forwarding buffer to children
//    APP_RECV_AND_FINISHED_DONE_FORWARDING
//    // app finished with buffer, done forwadin to children
//};
//
//// IAR is built on top of bcast, so it's not aware of BC related state changes, only cares about proposal state change.
//enum State_IAR {
//
//    //Proposal processing states
//    NEW_START_WAIT_VOTE, //the rank started a new proposal, waiting for votes
//
//    PASSIVE_RECV_APPROVED, // passive leaf rank recved a new proposal and approved, voting yes
//    PASSIVE_RECV_WAIT_VOTE, // passive non-leaf rank received a new proposal, and approved locally, in forwarding and will wait for children votes
//    PASSIVE_RECV_DECLINED, // passive newly received proposal get declined, and is voting no
//
//    ACTIVE_RECV_I_WIN,  //active rank received a conflicting proposal, and (local) wins the compete(), voting no
//    ACTIVE_RECV_I_LOSE, //lose compete, voting yes
//
//
//
//};
//
//int progress_BC(bcomm_BC_msg_t* msg);
//int progress_IAR(bcomm_IAR_msg_t* msg);
//
//#endif /* PROGRESS_ENGINE_H_ */
