#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include "../utils/global.h"
#include "MessageBuffer.h"
#include <string>
#include "../utils/communication.h"
#include "../utils/ydhdfs.h"
#include "../utils/Combiner.h"
#include "../utils/Aggregator.h"
#include <setjmp.h> //*** FT-change: to include setjmp/longjmp functions
using namespace std;

template<class VertexT, class AggregatorT = DummyAgg> //user-defined VertexT
class Worker {
	typedef vector<VertexT*> VertexContainer;
	typedef typename VertexContainer::iterator VertexIter;

	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::HashType HashT;

	typedef MessageBuffer<VertexT> MessageBufT;
	typedef typename MessageBufT::MessageContainerT MessageContainerT;
	typedef typename MessageBufT::Map Map;
	typedef typename MessageBufT::MapIter MapIter;

	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

public:
	Worker()
	{
		//init_workers();//put to run.cpp
		message_buffer = new MessageBuffer<VertexT>;
		global_message_buffer = message_buffer;
		active_count = 0;
		combiner = NULL;
		global_combiner = NULL;
		aggregator = NULL;
		global_aggregator = NULL;
		global_agg = NULL;
		global_CP_gap = 100000;//FT-change: checkpointing gap is very large by default
		//if you do not call setCPGap(gap), a job is not likely to be checkpointed (except for cp0)
	}

	//*** FT-change: do a checkpoint every "gap" supersteps
	void setCPGap(int gap)
	{
		global_CP_gap = gap;
	}

	void setCombiner(Combiner<MessageT>* cb) {
		combiner = cb;
		global_combiner = cb;
	}

	void setAggregator(AggregatorT* ag) {
		aggregator = ag;
		global_aggregator = ag;
		global_agg = new FinalT;
	}

	virtual ~Worker() {
		for (int i = 0; i < vertexes.size(); i++)
			delete vertexes[i];
		delete message_buffer;
		if (getAgg() != NULL)
			delete (FinalT*) global_agg;
		//worker_finalize();//put to run.cpp
		worker_barrier(); //newly added for ease of multi-job programming in run.cpp
	}

	//==================================
	//sub-functions
	void sync_graph() {
		//ResetTimer(4);
		//set send buffer
		vector<VertexContainer> _loaded_parts(_num_workers);
		for (int i = 0; i < vertexes.size(); i++) {
			VertexT* v = vertexes[i];
			_loaded_parts[hash(v->id)].push_back(v);
		}
		//exchange vertices to add
		all_to_all(_loaded_parts);

		//delete sent vertices
		for (int i = 0; i < vertexes.size(); i++) {
			VertexT* v = vertexes[i];
			if (hash(v->id) != _my_rank)
				delete v;
		}
		vertexes.clear();
		//collect vertices to add
		for (int i = 0; i < _num_workers; i++) {
			vertexes.insert(vertexes.end(), _loaded_parts[i].begin(),
					_loaded_parts[i].end());
		}
		_loaded_parts.clear();
		//StopTimer(4);
		//PrintTimer("Reduce Time",4);
	}

	void active_compute() {
		active_count = 0;
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			if (v_msgbufs[i].size() == 0) {
				if (vertexes[i]->is_active()) {
					vertexes[i]->compute(v_msgbufs[i]);
					AggregatorT* agg = (AggregatorT*) get_aggregator();
					if (agg != NULL)
						agg->stepPartial(vertexes[i]);
					if (vertexes[i]->is_active())
						active_count++;
				}
			} else {
				vertexes[i]->activate();
				vertexes[i]->compute(v_msgbufs[i]);
				v_msgbufs[i].clear(); //clear used msgs
				AggregatorT* agg = (AggregatorT*) get_aggregator();
				if (agg != NULL)
					agg->stepPartial(vertexes[i]);
				if (vertexes[i]->is_active())
					active_count++;
			}
		}
	}

	void all_compute() {
		active_count = 0;
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			vertexes[i]->activate();
			vertexes[i]->compute(v_msgbufs[i]);
			v_msgbufs[i].clear(); //clear used msgs
			AggregatorT* agg = (AggregatorT*) get_aggregator();
			if (agg != NULL)
				agg->stepPartial(vertexes[i]);
			if (vertexes[i]->is_active())
				active_count++;
		}
	}

	inline void add_vertex(VertexT* vertex) {
		vertexes.push_back(vertex);
		if (vertex->is_active())
			active_count++;
	}

	void agg_sync() {
		AggregatorT* agg = (AggregatorT*) get_aggregator();
		if (agg != NULL) {
			if (_my_rank != MASTER_RANK) { //send partialT to aggregator
				//gathering PartialT
				PartialT* part = agg->finishPartial();
				//------------------------ strategy choosing BEGIN ------------------------
				StartTimer(COMMUNICATION_TIMER);
				StartTimer(SERIALIZATION_TIMER);
				ibinstream m;
				m << part;
				int sendcount = m.size();
				StopTimer(SERIALIZATION_TIMER);
				int total = all_sum(sendcount);
				StopTimer(COMMUNICATION_TIMER);
				//------------------------ strategy choosing END ------------------------
				if (total <= AGGSWITCH)
					slaveGather(*part);
				else {
					send_ibinstream(m, MASTER_RANK);
				}
				//scattering FinalT
				slaveBcast(*((FinalT*) global_agg));
			} else {
				//------------------------ strategy choosing BEGIN ------------------------
				int total = all_sum(0);
				//------------------------ strategy choosing END ------------------------
				//gathering PartialT
				if (total <= AGGSWITCH) {
					vector<PartialT*> parts(_num_workers);
					masterGather(parts);
					for (int i = 0; i < _num_workers; i++) {
						if (i != MASTER_RANK) {
							PartialT* part = parts[i];
							agg->stepFinal(part);
							delete part;
						}
					}
				} else {
					for (int i = 0; i < _num_workers; i++) {
						if (i != MASTER_RANK) {
							obinstream um = recv_obinstream(i);
							PartialT* part;
							um >> part;
							agg->stepFinal(part);
							delete part;
						}
					}
				}
				//scattering FinalT
				FinalT* final = agg->finishFinal();
				//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
				*((FinalT*) global_agg) = *final; //deep copy
				masterBcast(*((FinalT*) global_agg));
			}
		}
	}

	//user-defined graphLoader ==============================
	virtual VertexT* toVertex(char* line) = 0; //this is what user specifies!!!!!!

	void load_vertex(VertexT* v) { //called by load_graph
		add_vertex(v);
	}

	void load_graph(const char* inpath) {
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while (true) {
			reader.readLine();
			if (!reader.eof())
				load_vertex(toVertex(reader.getLine()));
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
		//cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
	}
	//=======================================================

	//user-defined graphDumper ==============================
	virtual void toline(VertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!

	void dump_partition(const char* outpath) {
		hdfsFS fs = getHdfsFS();
		BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

		for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
			writer->check();
			toline(*it, *writer);
		}
		delete writer;
		hdfsDisconnect(fs);
	}

	//=======================================================

	//*** FT-change: load checkpoint
	void load_CP(string input_path)
	{
		//go back to CP superstep
		global_step_num = global_CP_step;
		//get cp_path
		char numbuf[5];
		sprintf(numbuf, "%d", global_CP_step);
		string cp_path = input_path + "_cp" + numbuf;
		sprintf(numbuf, "%d", _my_rank);
		cp_path = cp_path + "/part_" + numbuf;
		//load cp_data
		ResetTimer(IO_TIMER);
		hdfsFS fs = getHdfsFS();
		hdfsFile rhdl = getRHandle(cp_path.c_str(), fs);
		int fsize = hdfsAvailable(fs, rhdl);
		char* fbuf = new char[fsize];
		hdfsFullyRead(fs, rhdl, fbuf, fsize);
		hdfsCloseFile(fs, rhdl);
		hdfsDisconnect(fs);
		//parse to obinstream
		obinstream um(fbuf, fsize);
		for (int i = 0; i<vertexes.size(); i++) delete vertexes[i];//free vertices
		vertexes.clear();//free vertices
		//load to "vertexes"
		um >> vertexes;
		um >> global_bor_bitmap;
		for(int i=0; i<vertexes.size(); i++) if (vertexes[i]->is_active()) active_count++;//need to set active_count
		//prepare msgbuf
		if(!newly_respawned) message_buffer->clear_msgs();
		else message_buffer->init(vertexes);
		if(global_CP_step > 0)
		{//cp0 only involves vertexes
			//load to "messages", "aggregatedValue" if agg is used
			um >> message_buffer->v_msg_bufs;
			if(aggregator != NULL)
			{
				FinalT* aggVal = (FinalT*) global_agg;
				um >> (*aggVal);
			}
		}
		//timing
		worker_barrier(); //@@@@@@@@@@@@@ for accurate timing
		StopTimer(IO_TIMER);
		PrintTimer("- Reload Time", IO_TIMER);
	}

	//*** FT-change: write checkpoint
	//[simplification note]: current version force-writes cp-folders, while a sound system may need to check:
	//- whether the folder happens to be another data folder, and exit if so
	//- whether the folder is previously written but not committed due to a process failure detected by worker_barrier()
	void write_CP(string input_path)//write cpx, where x>0
	{
		ResetTimer(IO_TIMER);
		hdfsFS fs = getHdfsFS();
		char numbuf[5];
		sprintf(numbuf, "%d", global_step_num);
		string cpfile = input_path + "_cp" + numbuf;
		if(_my_rank == MASTER_RANK)
		{
			if (hdfsExists(fs, cpfile.c_str()) == 0) {
				if (hdfsDelete(fs, cpfile.c_str()) == -1) {
					fprintf(stderr, "Error deleting folder %s!\n", cpfile.c_str());
					exit(-1);
				}
			}
			int created = hdfsCreateDirectory(fs, cpfile.c_str());
			if (created == -1) {
				fprintf(stderr, "Failed to create current-cp folder %s!\n", cpfile.c_str());
				exit(-1);
			}
		}
		worker_barrier(); //*** FT-change: to check before writing the file
		//------
		ibinstream rbuf;
		rbuf << vertexes;
		rbuf << global_bor_bitmap;
		rbuf << message_buffer->v_msg_bufs;
		if(aggregator != NULL)
		{
			FinalT* aggVal = (FinalT*) global_agg;
			rbuf << (*aggVal);
		}
		//------
		sprintf(numbuf, "%d", _my_rank);
		cpfile = cpfile + "/part_" + numbuf;
		hdfsFile whdl = getWHandle(cpfile.c_str(), fs);
		tSize numWritten = hdfsWrite(fs, whdl, rbuf.get_buf(), rbuf.size());
		if (numWritten == -1) {
			fprintf(stderr, "Failed to write file %s!\n", cpfile.c_str());
			exit(-1);
		}
		if (hdfsFlush(fs, whdl)) {
			fprintf(stderr, "Failed to 'flush' %s\n", cpfile.c_str());
			exit(-1);
		}
		hdfsCloseFile(fs, whdl);
		worker_barrier(); //*** FT-change: for CP commit
		//commit the CP
		if(_my_rank == MASTER_RANK)
		{
			if(global_CP_step != 0)
			{//delete previous cp
				sprintf(numbuf, "%d", global_CP_step);
				cpfile = input_path + "_cp" + numbuf;
				if (hdfsExists(fs, cpfile.c_str()) == 0) {
					if (hdfsDelete(fs, cpfile.c_str()) == -1) {
						fprintf(stderr, "Error deleting prev-cp %s!\n", cpfile.c_str());
						exit(-1);
					}
				}
			}
		}
		hdfsDisconnect(fs);
		global_CP_step = global_step_num;
		StopTimer(IO_TIMER);
		PrintTimer("CP-Dump Time", IO_TIMER);
	}

	//=======================================================

	//independent loading module
	void dataLoad(const WorkerParams& params)
	{
		//*** FT-change: new loading logic
		//- if inputfolder_cp0 exists, load it directly
		//- otherwise, load inputfolder, shuffle vertices, and dump to inputfolder_cp0
		hdfsFS fs = getHdfsFS();
		init_timers();
		string shuffled_input = params.input_path + "_cp0";
		if (hdfsExists(fs, shuffled_input.c_str()) != 0) {
			//check path + init
			if (_my_rank == MASTER_RANK) {
				printf("\"%s\" not detected, read and shuffle \"%s\"...\n", shuffled_input.c_str(), params.input_path.c_str());
				if (dirCheck(params.input_path.c_str(), params.output_path.c_str(),
						_my_rank == MASTER_RANK, params.force_write) == -1)
					exit(-1);
			}
			//dispatch splits
			ResetTimer(WORKER_TIMER);
			if (_my_rank == MASTER_RANK) {
				vector<vector<string> >* arrangement =
						params.native_dispatcher ?
								dispatchLocality(params.input_path.c_str()) :
								dispatchRan(params.input_path.c_str());
				//reportAssignment(arrangement);//DEBUG !!!!!!!!!!
				masterScatter(*arrangement);
				vector<string>& assignedSplits = (*arrangement)[0];
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
						it != assignedSplits.end(); it++)
					load_graph(it->c_str());
				delete arrangement;
			} else {
				vector<string> assignedSplits;
				slaveScatter(assignedSplits);
				//reading assigned splits (map)
				for (vector<string>::iterator it = assignedSplits.begin();
						it != assignedSplits.end(); it++)
					load_graph(it->c_str());
			}

			//send vertices according to hash_id (reduce)
			sync_graph();
			//barrier for data loading
			worker_barrier(); //@@@@@@@@@@@@@ for accurate timing
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);
			//*** FT-change: dump shuffled
			ResetTimer(WORKER_TIMER);
			ibinstream rbuf;
			rbuf << vertexes;
			rbuf << global_bor_bitmap;
			hdfsFS fs = getHdfsFS();
			char numbuf[5];
			sprintf(numbuf, "%d", _my_rank);
			string shuffled_file = shuffled_input + "/part_" + numbuf;
			hdfsFile whdl = getWHandle(shuffled_file.c_str(), fs);
			tSize numWritten = hdfsWrite(fs, whdl, rbuf.get_buf(), rbuf.size());
			if (numWritten == -1) {
				fprintf(stderr, "Failed to write file %s!\n", shuffled_file.c_str());
				exit(-1);
			}
			if (hdfsFlush(fs, whdl)) {
				fprintf(stderr, "Failed to 'flush' %s\n", shuffled_file.c_str());
				exit(-1);
			}
			hdfsCloseFile(fs, whdl);
			worker_barrier(); //@@@@@@@@@@@@@ for accurate timing
			StopTimer(WORKER_TIMER);
			PrintTimer("Shuffled_Dump Time", WORKER_TIMER);
		}
		else
		{
			if (_my_rank == MASTER_RANK) printf("\"%s\" detected, directly loading...\n", shuffled_input.c_str());
			//direct loading
			ResetTimer(WORKER_TIMER);
			char numbuf[5];
			sprintf(numbuf, "%d", _my_rank);
			string shuffled_file = shuffled_input + "/part_" + numbuf;
			hdfsFile rhdl = getRHandle(shuffled_file.c_str(), fs);
			int fsize = hdfsAvailable(fs, rhdl);
			char* fbuf = new char[fsize];
			hdfsFullyRead(fs, rhdl, fbuf, fsize);
			hdfsCloseFile(fs, rhdl);
			//parse to array "vertexes"
			obinstream um(fbuf, fsize);
			um >> vertexes;
			um >> global_bor_bitmap;
			for(int i=0; i<vertexes.size(); i++) if (vertexes[i]->is_active()) active_count++;//need to set active_count
			worker_barrier(); //@@@@@@@@@@@@@ for accurate timing
			StopTimer(WORKER_TIMER);
			PrintTimer("Load Time", WORKER_TIMER);
		}
		get_vnum() = all_sum(vertexes.size());//assume no vertex addition
		hdfsDisconnect(fs);
	}

	//=======================================================

	long long global_msg_num; //*** FT-change: have to be global, otherwise changes between "setjmp(global_env)" and "longjmp(global_env)" will be ignored

	// run the worker
	//*** FT-change: master should not fail
	//[simplification note] current version assumes that master won't fail, though one can maintain a secondary master
	void run(const WorkerParams& params) {
		//*** FT-change: for newly-respawned processes
		if(newly_respawned){
			load_CP(params.input_path);//recover worker state
		}
		else
		{
			//************************ normal process when getting started ***************************
			//*** FT-change: new loading logic
			//- if inputfolder_cp0 exists, load it directly
			//- otherwise, load inputfolder, shuffle vertices, and dump to inputfolder_cp0
			dataLoad(params);
			message_buffer->init(vertexes);
			//*** FT-change: in a sound system, if a worker fails, it should reload the files assigned to it
			//[simplification note] current version assumes that loading is successful, otherwise one may kill the program and rerun
			//****************************************************************************
			//=========================================================
			global_step_num = 0;
			global_CP_step = 0;
			//************************ normal process when getting started ***************************
		}
		init_timers();
		ResetTimer(WORKER_TIMER);
		//supersteps
		long long step_msg_num;
		global_msg_num = 0; //*** FT-change: have to be global, otherwise changes between "setjmp(global_env)" and "longjmp(global_env)" will be ignored
		int exception;
		//************************ FT-change ************************
		exception = setjmp(global_env);//every process rolls back if error happens
		if(exception)
		{//recovery logic (for normal process)
			if(_my_rank == MASTER_RANK)//stop & report recovery timing
			{
				StopTimer(RESPAWN_TIMER);
				PrintTimer("Respawning Time", RESPAWN_TIMER);
			}
			//error handling function should have already recovered "global_world"
			load_CP(params.input_path);//recover worker state
		}
		//************************ FT-change ************************
		bool loop_end = false;
		int wakeAll;
		char cond_bitmap;
		while (true) {
			cond_bitmap = all_bor(global_bor_bitmap);
			if (getBit(FORCE_TERMINATE_ORBIT, cond_bitmap) == 1) loop_end = true;
			else
			{
				wakeAll = getBit(WAKE_ALL_ORBIT, cond_bitmap);
				if (wakeAll == 0) {
					active_vnum() = all_sum(active_count);
					if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, cond_bitmap) == 0) loop_end = true; //all_halt AND no_msg
				} else active_vnum() = get_vnum();
			}
			//--- 2. end of a superstep
			//************************ FT-change ************************
			worker_barrier(); //*** FT-change: commit the current superstep: if any error happens, no one can pass here
			//************************ FT-change ************************
			//--- 3. report time
			StopTimer(STEP_TIMER);
			if (_my_rank == MASTER_RANK && global_step_num > 0) {
				cout << "Superstep " << global_step_num
						<< " done. Time elapsed: " << get_timer(STEP_TIMER) << " seconds"
						<< endl;
				cout << "#msgs: " << step_msg_num << endl;
			}
			if(loop_end) break;
			//--- 4. checkpointing
			if(global_step_num - global_CP_step >= global_CP_gap) write_CP(params.input_path);//write_CP() has barrier-calls that handle faults
			//--- 1. start of a superstep
			ResetTimer(STEP_TIMER);
			global_step_num++;
			//===================
			AggregatorT* agg = (AggregatorT*) get_aggregator();
			if (agg != NULL)
				agg->init();
			//===================
			clearBits();
			if (wakeAll == 1)
				all_compute();
			else
				active_compute();
			message_buffer->combine();
			step_msg_num = master_sum_LL(message_buffer->get_total_msg());
			if (_my_rank == MASTER_RANK) global_msg_num += step_msg_num;
			message_buffer->sync_messages();
			agg_sync();
		}
		worker_barrier(); //@@@@@@@@@@@@@ for accurate timing
		StopTimer(WORKER_TIMER);
		PrintTimer("Communication Time", COMMUNICATION_TIMER);
		PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
		PrintTimer("- Transfer Time", TRANSFER_TIMER);
		PrintTimer("Total Computational Time", WORKER_TIMER);
		if (_my_rank == MASTER_RANK)
			cout << "Total #msgs=" << global_msg_num << endl;

		// dump graph
		ResetTimer(WORKER_TIMER);
		dump_partition(params.output_path.c_str());
		StopTimer(WORKER_TIMER);
		PrintTimer("Dump Time", WORKER_TIMER);
		//*** FT-change: delete last CP
		if(_my_rank == MASTER_RANK)
		{
			if(global_CP_step != 0)
			{//delete previous cp
				char numbuf[5];
				sprintf(numbuf, "%d", global_CP_step);
				string lastCP = params.input_path + "_cp" + numbuf;
				hdfsFS fs = getHdfsFS();
				if (hdfsDelete(fs, lastCP.c_str()) == -1) {
					fprintf(stderr, "Error deleting last-cp %s!\n", lastCP.c_str());
					exit(-1);
				}
				hdfsDisconnect(fs);
			}
		}
	}

private:
	HashT hash;
	VertexContainer vertexes;
	int active_count;

	MessageBuffer<VertexT>* message_buffer;
	Combiner<MessageT>* combiner;
	AggregatorT* aggregator;
};

#endif
