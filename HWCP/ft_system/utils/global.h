#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <limits.h>
#include <string>
#include <ext/hash_set>
#include <ext/hash_map>
#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set
#include <setjmp.h> //*** FT-change: to include setjmp/longjmp functions
#include <mpi-ext.h> //*** FT-change: to include MPIX_... functions
#include "time.h"
using namespace std;

//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

//============================
//global variables
int global_vnum = 0;
inline int& get_vnum()
{
    return global_vnum;
}
int global_active_vnum = 0;
inline int& active_vnum()
{
    return global_active_vnum;
}

//*******************************************************
//*** FT-change: functions below are for fault-tolerance
MPI_Comm global_world;//the working comm
bool newly_respawned;//whether the current process is newly spawned
char** global_argv;//program name for respawning
int global_CP_step;//checkpoint superstep
int global_CP_gap;//checkpointing every "global_CP_gap" supersteps

MPI_Errhandler global_err_handler;
jmp_buf global_env;//recovery environment
void errhandling_func(MPI_Comm* pcomm, int* prc, ...)
//for alive functions to call, after detecting dead processes
//[simplification note] current version assumes that no alive processes would die (again) during the call of errhandling_func()
{
	if(_my_rank == MASTER_RANK) ResetTimer(RESPAWN_TIMER);//start recovery timing
	MPI_Comm shrinked, icomm, mcomm;
	MPI_Group cgrp, sgrp, dgrp;
	int ns, srank, nd, drank;
	//------ report detected error
	char errstr[MPI_MAX_ERROR_STRING];
	int len;
	MPI_Error_string(*prc, errstr, &len);
	cout<<_my_rank<<": [error detected] "<<errstr<<endl;
	//------ shrink
	MPIX_Comm_revoke(global_world);//IMPORTANT: immediately notify others before shrinking (to ensure that revoke can be reached)
	MPIX_Comm_shrink(global_world, &shrinked);
	MPI_Comm_size(shrinked, &ns);
	MPI_Comm_rank(shrinked, &srank);
	//------ spawn replacement processes
	MPI_Comm_spawn(global_argv[0], &global_argv[1], _num_workers - ns, MPI_INFO_NULL, 0, shrinked, &icomm, MPI_ERRCODES_IGNORE);
	//rank 0 of "shrinked" tells spawnees their ranks
	if(srank == 0)
	{
		//get dead processes
		MPI_Comm_group(global_world, &cgrp);
		MPI_Comm_group(shrinked, &sgrp);
		MPI_Group_difference(cgrp, sgrp, &dgrp);
		MPI_Group_size(dgrp, &nd);
		//compute ranks of spawnees
		for(int i=0; i<nd; i++) {
			MPI_Group_translate_ranks(dgrp, 1, &i, cgrp, &drank);
			MPI_Send(&drank, 1, MPI_INT, i, 0, icomm);//send drank
			MPI_Send(&global_CP_step, 1, MPI_INT, i, 0, icomm);//send checkpoint superstep
			MPI_Send(&get_vnum(), 1, MPI_INT, i, 0, icomm);//send vnum
		}
		MPI_Group_free(&cgrp);
		MPI_Group_free(&sgrp);
		MPI_Group_free(&dgrp);
	}
	MPI_Comm_free(&shrinked);
	//------ merge intercomm
	MPI_Intercomm_merge(icomm, 1, &mcomm);
	MPI_Comm_free(&icomm);
	//------ reorder ranks
	MPI_Comm_split(mcomm, 1, _my_rank, &global_world);
	MPI_Comm_free(&mcomm);
	//------ set error handler for recovered comm
	MPI_Comm_create_errhandler(errhandling_func, &global_err_handler);
	MPI_Comm_set_errhandler(global_world, global_err_handler);
	//------
	newly_respawned = false; //even though it may have parent_comm (spawned by a previous failure), after the error handling it's a normal process
	//------ go to specified recovery point
	longjmp(global_env, 1);
}
//*******************************************************

//*******************************************************
//*** FT-change: to know the program name for respawn
void init_workers(int* argc, char** argv[])
{
    MPI_Init(argc, argv);
    global_argv = *argv;
    //------
    MPI_Comm_get_parent(&global_world);
    if(global_world == MPI_COMM_NULL)
    {//initial process
    	newly_respawned = false;
        MPI_Comm_dup(MPI_COMM_WORLD, &global_world);
    }
	else
	{//spawnee
		newly_respawned = true;
		//------
		MPI_Comm mcomm;
		//now "global_world" is parent_comm
		MPI_Recv(&_my_rank, 1, MPI_INT, 0, 0, global_world, MPI_STATUS_IGNORE);
		MPI_Recv(&global_CP_step, 1, MPI_INT, 0, 0, global_world, MPI_STATUS_IGNORE);
		MPI_Recv(&get_vnum(), 1, MPI_INT, 0, 0, global_world, MPI_STATUS_IGNORE);
		//merge intercomm
		MPI_Intercomm_merge(global_world, 1, &mcomm);
		//reorder ranks
		MPI_Comm_split(mcomm, 1, _my_rank, &global_world);
		MPI_Comm_free(&mcomm);
	}
    //------
	MPI_Comm_size(global_world, &_num_workers);
	MPI_Comm_rank(global_world, &_my_rank);
	MPI_Comm_create_errhandler(errhandling_func, &global_err_handler);
	MPI_Comm_set_errhandler(global_world, global_err_handler);
}
//*******************************************************

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(global_world);
}

//------------------------
// worker parameters

struct WorkerParams {
    string input_path;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams()
    {
        force_write = true;
        native_dispatcher = false;
    }
};

struct MultiInputParams {
    vector<string> input_paths;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    MultiInputParams()
    {
        force_write = true;
        native_dispatcher = false;
    }

    void add_input_path(string path)
    {
        input_paths.push_back(path);
    }
};

//============================
//general types
typedef int VertexID;

//============================
//global variables
int global_step_num;
inline int step_num()
{
    return global_step_num;
}

int global_phase_num;
inline int phase_num()
{
    return global_phase_num;
}

void* global_message_buffer = NULL;
inline void set_message_buffer(void* mb)
{
    global_message_buffer = mb;
}
inline void* get_message_buffer()
{
    return global_message_buffer;
}

void* global_combiner = NULL;
inline void set_combiner(void* cb)
{
    global_combiner = cb;
}
inline void* get_combiner()
{
    return global_combiner;
}

void* global_aggregator = NULL;
inline void set_aggregator(void* ag)
{
    global_aggregator = ag;
}
inline void* get_aggregator()
{
    return global_aggregator;
}

void* global_agg = NULL; //for aggregator, FinalT of last round
inline void* getAgg()
{
    return global_agg;
}

enum BITS {
    HAS_MSG_ORBIT = 0,
    FORCE_TERMINATE_ORBIT = 1,
    WAKE_ALL_ORBIT = 2
};
//currently, only 3 bits are used, others can be defined by users
char global_bor_bitmap;

void clearBits()
{
    global_bor_bitmap = 0;
}

void setBit(int bit)
{
    global_bor_bitmap |= (2 << bit);
}

int getBit(int bit, char bitmap)
{
    return ((bitmap & (2 << bit)) == 0) ? 0 : 1;
}

void hasMsg()
{
    setBit(HAS_MSG_ORBIT);
}

void wakeAll()
{
    setBit(WAKE_ALL_ORBIT);
}

void forceTerminate()
{
    setBit(FORCE_TERMINATE_ORBIT);
}

#endif
