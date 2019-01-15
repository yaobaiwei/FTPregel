#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <mpi.h>
#include "time.h"
#include "serialization.h"
#include "global.h"

//============================================
//Allreduce
int all_sum(int my_copy)
{
    int tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_INT, MPI_SUM, global_world);
    return tmp;
}

long long master_sum_LL(long long my_copy)
{
    long long tmp = 0;
    MPI_Reduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, MASTER_RANK, global_world);
    return tmp;
}

long long all_sum_LL(long long my_copy)
{
    long long tmp = 0;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_LONG_LONG_INT, MPI_SUM, global_world);
    return tmp;
}

char all_bor(char my_copy)
{
    char tmp;
    MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_BOR, global_world);
    return tmp;
}

/*
bool all_lor(bool my_copy){
	bool tmp;
	MPI_Allreduce(&my_copy, &tmp, 1, MPI_BYTE, MPI_LOR, global_world);
	return tmp;
}
*/

//============================================
//char-level send/recv
void pregel_send(void* buf, int size, int dst)
{
    MPI_Send(buf, size, MPI_CHAR, dst, 0, global_world);
}

void pregel_recv(void* buf, int size, int src)
{
    MPI_Recv(buf, size, MPI_CHAR, src, 0, global_world, MPI_STATUS_IGNORE);
}

//============================================
//binstream-level send/recv
void send_ibinstream(ibinstream& m, int dst)
{
    size_t size = m.size();
    pregel_send(&size, sizeof(size_t), dst);
    pregel_send(m.get_buf(), m.size(), dst);
}

obinstream recv_obinstream(int src)
{
    size_t size;
    pregel_recv(&size, sizeof(size_t), src);
    char* buf = new char[size];
    pregel_recv(buf, size, src);
    return obinstream(buf, size);
}

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst)
{
    ibinstream m;
    m << data;
    send_ibinstream(m, dst);
}

template <class T>
T recv_data(int src)
{
    obinstream um = recv_obinstream(src);
    T data;
    um >> data;
    return data;
}

//============================================
//all-to-all
//*** FT-change-2: to check global_state_vec when sending
template <class T>
void all_to_all(std::vector<T>& to_exchange)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
                StartTimer(SERIALIZATION_TIMER);
                //send
                if(global_state_vec[partner] <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	ibinstream m;
					m << to_exchange[partner];
					StopTimer(SERIALIZATION_TIMER);
					StartTimer(TRANSFER_TIMER);
					send_ibinstream(m, partner);
					StopTimer(TRANSFER_TIMER);
                }
                //receive
                if(global_state_step <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	StartTimer(TRANSFER_TIMER);
					obinstream um = recv_obinstream(partner);
					StopTimer(TRANSFER_TIMER);
					StartTimer(SERIALIZATION_TIMER);
					um >> to_exchange[partner];
					StopTimer(SERIALIZATION_TIMER);
                }
            } else {
                StartTimer(TRANSFER_TIMER);
                //receive
                T received;
                if(global_state_step <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	obinstream um = recv_obinstream(partner);
					StopTimer(TRANSFER_TIMER);
					StartTimer(SERIALIZATION_TIMER);
					um >> received;
                }
                //send
                if(global_state_vec[partner] <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	ibinstream m;
					m << to_exchange[partner];
					StopTimer(SERIALIZATION_TIMER);
					StartTimer(TRANSFER_TIMER);
					send_ibinstream(m, partner);
					StopTimer(TRANSFER_TIMER);
                }
                if(global_state_step <= step_num()) to_exchange[partner] = received;//*** FT-change-2: do not send if state[dst] > superstep
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

template <class T, class T1>
void all_to_all(vector<T>& to_send, vector<T1>& to_get)
{
    StartTimer(COMMUNICATION_TIMER);
    //for each to_exchange[i]
    //        send out *to_exchange[i] to i
    //        save received data in *to_exchange[i]
    int np = get_num_workers();
    int me = get_worker_id();
    for (int i = 0; i < np; i++) {
        int partner = (i - me + np) % np;
        if (me != partner) {
            if (me < partner) {
            	//send
            	if(global_state_vec[partner] <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
            	{
            		StartTimer(SERIALIZATION_TIMER);
					ibinstream m;
					m << to_send[partner];
					StopTimer(SERIALIZATION_TIMER);
					StartTimer(TRANSFER_TIMER);
					send_ibinstream(m, partner);
					StopTimer(TRANSFER_TIMER);
            	}
                //receive
            	if(global_state_step <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
            	{
            		StartTimer(TRANSFER_TIMER);
					obinstream um = recv_obinstream(partner);
					StopTimer(TRANSFER_TIMER);
					StartTimer(SERIALIZATION_TIMER);
					um >> to_get[partner];
					StopTimer(SERIALIZATION_TIMER);
            	}
            } else {
                T1 received;
                //receive
                if(global_state_step <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	StartTimer(TRANSFER_TIMER);
                	obinstream um = recv_obinstream(partner);
                	StopTimer(TRANSFER_TIMER);
					StartTimer(SERIALIZATION_TIMER);
					um >> received;
                }
                //send
                if(global_state_vec[partner] <= step_num())//*** FT-change-2: do not send if state[dst] > superstep
                {
                	ibinstream m;
					m << to_send[partner];
					StopTimer(SERIALIZATION_TIMER);
					StartTimer(TRANSFER_TIMER);
					send_ibinstream(m, partner);
					StopTimer(TRANSFER_TIMER);
                }
                if(global_state_step <= step_num()) to_get[partner] = received;//*** FT-change-2: do not send if state[dst] > superstep
            }
        }
    }
    StopTimer(COMMUNICATION_TIMER);
}

//============================================
//scatter
template <class T>
void masterScatter(vector<T>& to_send)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts = new int[_num_workers];
    int recvcount;
    int* sendoffset = new int[_num_workers];

    ibinstream m;
    StartTimer(SERIALIZATION_TIMER);
    int size = 0;
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank) {
            sendcounts[i] = 0;
        } else {
            m << to_send[i];
            sendcounts[i] = m.size() - size;
            size = m.size();
        }
    }
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    for (int i = 0; i < _num_workers; i++) {
        sendoffset[i] = (i == 0 ? 0 : sendoffset[i - 1] + sendcounts[i - 1]);
    }
    char* sendbuf = m.get_buf(); //ibinstream will delete it
    char* recvbuf;

    StartTimer(TRANSFER_TIMER);
    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    delete[] sendcounts;
    delete[] sendoffset;
    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveScatter(T& to_get)
{ //scatter
    StartTimer(COMMUNICATION_TIMER);
    int* sendcounts;
    int recvcount;
    int* sendoffset;

    StartTimer(TRANSFER_TIMER);
    MPI_Scatter(sendcounts, 1, MPI_INT, &recvcount, 1, MPI_INT, MASTER_RANK, global_world);

    char* sendbuf;
    char* recvbuf = new char[recvcount]; //obinstream will delete it

    MPI_Scatterv(sendbuf, sendcounts, sendoffset, MPI_CHAR, recvbuf, recvcount, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, recvcount);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);
    StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//gather
template <class T>
void masterGather(vector<T>& to_get)
{ //gather
    StartTimer(COMMUNICATION_TIMER);
    int sendcount = 0;
    int* recvcounts = new int[_num_workers];
    int* recvoffset = new int[_num_workers];

    StartTimer(TRANSFER_TIMER);
    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    for (int i = 0; i < _num_workers; i++) {
        recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
    }

    char* sendbuf;
    int recv_tot = recvoffset[_num_workers - 1] + recvcounts[_num_workers - 1];
    char* recvbuf = new char[recv_tot]; //obinstream will delete it

    StartTimer(TRANSFER_TIMER);
    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, recv_tot);
    for (int i = 0; i < _num_workers; i++) {
        if (i == _my_rank)
            continue;
        um >> to_get[i];
    }
    StopTimer(SERIALIZATION_TIMER);
    delete[] recvcounts;
    delete[] recvoffset;
    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveGather(T& to_send)
{ //gather
    StartTimer(COMMUNICATION_TIMER);
    int sendcount;
    int* recvcounts;
    int* recvoffset;

    StartTimer(SERIALIZATION_TIMER);
    ibinstream m;
    m << to_send;
    sendcount = m.size();
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    char* sendbuf = m.get_buf(); //ibinstream will delete it
    char* recvbuf;

    StartTimer(TRANSFER_TIMER);
    MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);
    StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//bcast
template <class T>
void masterBcast(T& to_send)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    ibinstream m;
    m << to_send;
    int size = m.size();
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, global_world);

    char* sendbuf = m.get_buf();
    MPI_Bcast(sendbuf, size, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void slaveBcast(T& to_get)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    int size;

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(TRANSFER_TIMER);
    char* recvbuf = new char[size]; //obinstream will delete it
    MPI_Bcast(recvbuf, size, MPI_CHAR, MASTER_RANK, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, size);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

//================================================================
//bcast (root is not master, but _alive_rank)
template <class T>
void sendBcast(T& to_send)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    ibinstream m;
    m << to_send;
    int size = m.size();
    StopTimer(SERIALIZATION_TIMER);

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, _alive_rank, global_world);

    char* sendbuf = m.get_buf();
    MPI_Bcast(sendbuf, size, MPI_CHAR, _alive_rank, global_world);
    StopTimer(TRANSFER_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

template <class T>
void recvBcast(T& to_get)
{ //broadcast
    StartTimer(COMMUNICATION_TIMER);

    int size;

    StartTimer(TRANSFER_TIMER);
    MPI_Bcast(&size, 1, MPI_INT, _alive_rank, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(TRANSFER_TIMER);
    char* recvbuf = new char[size]; //obinstream will delete it
    MPI_Bcast(recvbuf, size, MPI_CHAR, _alive_rank, global_world);
    StopTimer(TRANSFER_TIMER);

    StartTimer(SERIALIZATION_TIMER);
    obinstream um(recvbuf, size);
    um >> to_get;
    StopTimer(SERIALIZATION_TIMER);

    StopTimer(COMMUNICATION_TIMER);
}

#endif
