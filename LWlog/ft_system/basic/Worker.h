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

template<class T>
void safe_add(int step, T val, vector<T> & vec)
{
	if(vec.size() < step) vec.resize(step);
	vec.push_back(val);
}

template<class VertexT, class AggregatorT = DummyAgg> //user-defined VertexT
class Worker {
	typedef vector<VertexT*> VertexContainer;
	typedef typename VertexContainer::iterator VertexIter;

	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::ValueType ValueT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::HashType HashT;

	typedef MessageBuffer<VertexT> MessageBufT;
	typedef typename MessageBufT::MessageContainerT MessageContainerT;
	typedef typename MessageBufT::Map Map;
	typedef typename MessageBufT::MapIter MapIter;
	typedef typename MessageBufT::Vec Vec;
	typedef typename MessageBufT::VecGroup VecGroup;

	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

public:
	//*** FT-change-3: buffers for holding locally loaded vertex info
	vector<ValueT> val_vec;
	//another vector is "vstate_before_update"

	Worker()
	{
		//init_workers();//put to run.cpp
		message_buffer = new MessageBuffer<VertexT>;
		global_message_buffer = message_buffer;
		active_count = 0;
		global_bor_bitmap = 0;
		combiner = NULL;
		global_combiner = NULL;
		aggregator = NULL;
		global_aggregator = NULL;
		global_agg = NULL;
		global_state_vec.resize(_num_workers);//*** FT-change-2: allocate space to state-step vec
		cp_disabled = false;//FT-change: to allow checkpointing by default
		global_CP_gap = 100000;//FT-change: checkpointing gap is very large by default
		_alive_rank = 0;//*** FT-change-2
		rwthread = NULL;//*** FT-change-2
		//if you do not call setCPGap(gap), a job is not likely to be checkpointed (except for cp0)
		//*** FT-change-CPlight
		global_value_copy = new ValueT;
		global_need_copy = false;
		global_value_vec = &val_vec;//registered as global variable
		use_backup_values = false;
	}

	//*** FT-change-4: new UDF
	virtual bool setCPdisable()
	{
		return false;//default setting
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
		//*** FT-change-CPlight
		delete (ValueT *)global_value_copy;
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

	void cpload_compute() {
		global_need_copy = true;
		MessageContainerT dummy;
		for (global_vpos = 0; global_vpos < vertexes.size(); global_vpos++) {
			if(vstate_before_update[global_vpos]) vertexes[global_vpos]->compute(dummy);
		}
		global_need_copy = false;
		message_buffer->combine(); //*** FT-change-2: combine msgs that are written out
	}

	void active_compute() {
		active_count = 0;
		vstate_before_update.clear();//*** FT-change-LWCP
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			if (v_msgbufs[i].size() == 0) {
				vstate_before_update.push_back(vertexes[i]->is_active());//*** FT-change-LWCP
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
				vstate_before_update.push_back(true);//*** FT-change-LWCP
				vertexes[i]->compute(v_msgbufs[i]);
				v_msgbufs[i].clear(); //clear used msgs
				AggregatorT* agg = (AggregatorT*) get_aggregator();
				if (agg != NULL)
					agg->stepPartial(vertexes[i]);
				if (vertexes[i]->is_active())
					active_count++;
			}
		}
		//---------------------------------
		global_state_step++; //FT-change-2: if v-compute is called, state is forwarded
		//*** FT-change-2: record states before communication/sync
		AggregatorT* agg = (AggregatorT*) get_aggregator();
		if (agg != NULL) {
			PartialT* aggVal = agg->finishPartial();
			safe_add<PartialT>(step_num(), *aggVal, partialAggVec);
		}
		safe_add<char>(step_num(), global_bor_bitmap, partialCondVec);
		message_buffer->combine(); //*** FT-change-2: combine msgs that are written out
		//*** FT-change-3: switch between value-logging and msg-logging
		if(cp_disabled) rwthread = new thread(&MessageBuffer<VertexT>::local_save, message_buffer);
		else rwthread = new thread(&Worker<VertexT, AggregatorT>::local_save, this);
		//*** FT-change-3:
		safe_add<char>(step_num(), cp_disabled, cpDisableVec);
	}

	void all_compute() {
		active_count = 0;
		vstate_before_update.clear();//*** FT-change-LWCP
		MessageBufT* mbuf = (MessageBufT*) get_message_buffer();
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int i = 0; i < vertexes.size(); i++) {
			vertexes[i]->activate();
			vstate_before_update.push_back(true);//*** FT-change-LWCP
			vertexes[i]->compute(v_msgbufs[i]);
			v_msgbufs[i].clear(); //clear used msgs
			AggregatorT* agg = (AggregatorT*) get_aggregator();
			if (agg != NULL)
				agg->stepPartial(vertexes[i]);
			if (vertexes[i]->is_active())
				active_count++;
		}
		//---------------------------------
		global_state_step++; //FT-change-2: if v-compute is called, state is forwarded
		//*** FT-change-2: record states before communication/sync
		AggregatorT* agg = (AggregatorT*) get_aggregator();
		if (agg != NULL) {
			PartialT* aggVal = agg->finishPartial();
			safe_add<PartialT>(step_num(), *aggVal, partialAggVec);
		}
		safe_add<char>(step_num(), global_bor_bitmap, partialCondVec);
		message_buffer->combine(); //*** FT-change-2: combine msgs that are written out
		if(cp_disabled) rwthread = new thread(&MessageBuffer<VertexT>::local_save, message_buffer);
		else rwthread = new thread(&Worker<VertexT, AggregatorT>::local_save, this);
		//*** FT-change-3:
		safe_add<char>(step_num(), cp_disabled, cpDisableVec);
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
				PartialT* part = &partialAggVec[step_num()];
				slaveGather(*part);
				//scattering FinalT
				slaveBcast(*((FinalT*) global_agg));
			} else {
				//gathering PartialT
				PartialT* master_part = &partialAggVec[step_num()];//FT-change-2: take from agg-vec
				agg->init();//FT-change-2: init since every one is taken from agg-vec, including master
				vector<PartialT*> parts(_num_workers);
				masterGather(parts);
				for (int i = 0; i < _num_workers; i++) {
					if (i != MASTER_RANK) {
						PartialT* part = parts[i];
						agg->stepFinal(part);
						delete part;
					}
					else agg->stepFinal(master_part);
				}
				//scattering FinalT
				FinalT* final = agg->finishFinal();
				//cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
				*((FinalT*) global_agg) = *final; //deep copy
				masterBcast(*((FinalT*) global_agg));
			}
			//-------------------------------
			//*** FT-change-2: record final_aggVal after sync
			safe_add<FinalT>(step_num(), *((FinalT*) global_agg), finalAggVec);
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
		//get cp_path
		char numbuf[5];
		sprintf(numbuf, "%d", global_CP_step);
		string cp_path0 = input_path + "_cp0";
		string cp_path = input_path + "_cp" + numbuf;
		sprintf(numbuf, "%d", _my_rank);
		cp_path0 = cp_path0 + "/part_" + numbuf;
		cp_path = cp_path + "/part_" + numbuf;
		//------
		ResetTimer(IO_TIMER);
		//load cp_0
		if(newly_respawned || global_CP_step == 0)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile rhdl = getRHandle(cp_path0.c_str(), fs);
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
		}
		//------
		//load cp_last
		if(global_CP_step > 0)
		{
			hdfsFS fs = getHdfsFS();
			hdfsFile rhdl = getRHandle(cp_path.c_str(), fs);
			int fsize = hdfsAvailable(fs, rhdl);
			char* fbuf = new char[fsize];
			hdfsFullyRead(fs, rhdl, fbuf, fsize);
			hdfsCloseFile(fs, rhdl);
			hdfsDisconnect(fs);
			//parse to obinstream
			obinstream um(fbuf, fsize);
			for (int i = 0; i<vertexes.size(); i++)
			{
				um >> vertexes[i]->value();
				um >> vertexes[i]->active;
			}
			for(int i=0; i<vertexes.size(); i++) if (vertexes[i]->is_active()) active_count++;//need to set active_count
			um >> global_bor_bitmap;
			um >> vstate_before_update;
			//prepare msgbuf
			message_buffer->init(vertexes);
			//load to "messages", "aggregatedValue" if agg is used
			if(aggregator != NULL)
			{
				FinalT* aggVal = (FinalT*) global_agg;
				um >> (*aggVal);
			}
			cpload_compute();
			step_msg_num = master_sum_LL(message_buffer->get_total_msg());
			if (_my_rank == MASTER_RANK) global_msg_num += step_msg_num;
			message_buffer->sync_messages();
			message_buffer->clear_sendBuf(); //*** FT-change-2: clear sending buffer
		}
		else
		{
			for(int i=0; i<vertexes.size(); i++) if (vertexes[i]->is_active()) active_count++;//need to set active_count
			if(newly_respawned) message_buffer->init(vertexes);
		}
		//timing
		StopTimer(IO_TIMER);
		cout << _my_rank << ": CP_load Time " << get_timer(IO_TIMER) << " seconds" << endl;
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
		for(int i=0; i<vertexes.size(); i++)
		{
			rbuf << vertexes[i]->value();
			rbuf << vertexes[i]->active;
		}
		rbuf << global_bor_bitmap;
		rbuf << vstate_before_update;
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
		//do not remove /tmp/ftpregel, since it is log of CP-step is needed for recovery of a survivor
		//***********************************************************
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

	//*** FT-change-2: to save v-state to local disk location: /tmp/ftpregel/#superstep/(_my_rank)_(tgt_rank)
	void local_save()
	{
		if(newly_respawned) ResetTimer(IO_TIMER);
		//for async call
		char numbuf[5];
		sprintf(numbuf, "%d", global_step_num);
		string save_path;
		save_path += "/tmp/ftpregel/";
		save_path += numbuf;
		_mkdir(save_path.c_str());//create local directory
		sprintf(numbuf, "%d", _my_rank);
		save_path = save_path + "/" + numbuf;
		//------
		ofstream out(save_path.c_str());
		ibinstream rbuf;
		rbuf << vertexes.size();
		for(int j=0; j<vertexes.size(); j++) rbuf << vertexes[j]->value();
		rbuf << vstate_before_update;
		out.write(rbuf.get_buf(), rbuf.size());
		out.close();
		if(newly_respawned)
		{
			StopTimer(IO_TIMER);
			cout << _my_rank << "'s msg-logging time: " << get_timer(IO_TIMER) << endl;
		}
	}

	//*** FT-change-2: to load v-state from local disk location: /tmp/ftpregel/#superstep/_my_rank
	void load_rbuf()//remember to free_rbuf to avoid mem-leak!!!
	{
		//for async call
		char numbuf[5];
		sprintf(numbuf, "%d", step_num());
		string save_path;
		save_path += "/tmp/ftpregel/";
		save_path += numbuf;
		sprintf(numbuf, "%d", _my_rank);
		save_path = save_path + "/" + numbuf;
		//------
		//get file size
		struct stat statbuf;
		stat(save_path.c_str(), &statbuf);
		int fsize = statbuf.st_size;
		char* fbuf = new char[fsize];
		//read msgs
		ifstream in(save_path.c_str());
		in.read(fbuf, fsize);
		in.close();
		//parse to sendBuf
		obinstream oMsgStream(fbuf, fsize);
		oMsgStream >> val_vec;
		oMsgStream >> vstate_before_update;
	}

	long long global_msg_num; //*** FT-change: have to be global, otherwise changes between "setjmp(global_env)" and "longjmp(global_env)" will be ignored
	long long step_msg_num;
	bool vcomp_called;

	// run the worker
	//*** FT-change: master should not fail
	//[simplification note] current version assumes that master won't fail, though one can maintain a secondary master
	void run(const WorkerParams& params) {
		//*** FT-change: for newly-respawned processes
		if(newly_respawned){
			MPI_Allgather(&global_state_step, 1, MPI_INT, &global_state_vec[0], 1, MPI_INT, global_world);//*** FT-change-2: sync state-step
			//*** FT-change-2: vote for _alive_rank
			int max=-1;
			for(int i=0; i<_num_workers; i++)
			{
				if(global_state_vec[i] > max)
				{
					max = global_state_vec[i];
					_alive_rank = i;
				}
			}
			load_CP(params.input_path);//recover worker state
			if(global_agg != NULL)
			{
				if(global_CP_step>0)
				{
					//*** FT-change-2: after recovery, survivor get finAgg from agg-vec
					FinalT & finAgg = *((FinalT*) global_agg);
					safe_add<FinalT>(step_num(), finAgg, finalAggVec);
					recvBcast(finalAggVec[step_num()]);
				}
			}
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
			partialCondVec.push_back(global_bor_bitmap);
			//************************ normal process when getting started ***************************
		}
		init_timers();
		ResetTimer(WORKER_TIMER);
		//supersteps
		global_msg_num = 0; //*** FT-change: have to be global, otherwise changes between "setjmp(global_env)" and "longjmp(global_env)" will be ignored
		int exception;
		//************************ FT-change ************************
		exception = setjmp(global_env);//every process rolls back if error happens
		if(exception)
		{//recovery logic (for normal process)
			MPI_Allgather(&global_state_step, 1, MPI_INT, &global_state_vec[0], 1, MPI_INT, global_world);//*** FT-change-2: sync state-step
			//*** FT-change-2: vote for _alive_rank
			int max=-1;
			for(int i=0; i<_num_workers; i++)
			{
				if(global_state_vec[i] > max)
				{
					max = global_state_vec[i];
					_alive_rank = i;
				}
			}
			if(_my_rank == MASTER_RANK)//stop & report recovery timing
			{
				StopTimer(RESPAWN_TIMER);
				PrintTimer("Respawning Time", RESPAWN_TIMER);
			}
			//error handling function should have already recovered "global_world"
			//*** FT-change-2: no need to load CP
			message_buffer->clear_msgs();//!!! IMPORTANT !!! need to emptize msg_buf because msg_sync() may be half-done
			//******************************************************************************************************

			//*** FT-change-3: load log[CP-step]
			if(global_CP_step > 0)
			{
				load_rbuf();//v-state
				use_backup_values = true;
				cpload_compute();
				step_msg_num = master_sum_LL(message_buffer->get_total_msg());
				if (_my_rank == MASTER_RANK) global_msg_num += step_msg_num;
				use_backup_values = false;
				message_buffer->sync_messages();
				message_buffer->clear_sendBuf(); //*** FT-change-3: clear sending buffer
				//******************************************************************************************************
				//*** FT-change-2: after recovery, survivor get finAgg from agg-vec
				if(global_agg != NULL)
				{

					*((FinalT*) global_agg) = finalAggVec[step_num()];
					if(_my_rank == _alive_rank)
					{
						sendBcast(finalAggVec[step_num()]);
					}
					else
					{
						recvBcast(finalAggVec[step_num()]);
					}
				}
			}
			else//should load log[0], which we didn't implement for simplicity; instead, we let everybody roll back to CP[0]
			{
				load_CP(params.input_path);//recover worker state
				global_state_step = global_CP_step;//roll back state also
			}
		}
		//************************ FT-change ************************
		bool loop_end = false;
		int wakeAll;
		char cond_bitmap;
		while (true) {
			if(state_consistent())//FT-change-2: state inconsistent, then still in recovery, cannot terminate anyway
			{
				cond_bitmap = all_bor(partialCondVec[step_num()]);
				if (getBit(FORCE_TERMINATE_ORBIT, cond_bitmap) == 1) loop_end = true;
				else
				{
					wakeAll = getBit(WAKE_ALL_ORBIT, cond_bitmap);
					if (wakeAll == 0) {
						active_vnum() = all_sum(active_count);
						if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, cond_bitmap) == 0) loop_end = true; //all_halt AND no_msg
					} else active_vnum() = get_vnum();
				}
			}
			else loop_end = false;
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
			if(!cp_disabled)//FT-change: to write checkpoint, !!! put here so that if the last round terminates, no checkpoint will be written
			{
				if(global_step_num - global_CP_step >= global_CP_gap) write_CP(params.input_path);//write_CP() has barrier-calls that handle faults
			}
			//--- 1. start of a superstep
			ResetTimer(STEP_TIMER);
			global_step_num++;
			cp_disabled = setCPdisable();//*** FT-change-4
			//===================
			if(global_step_num > global_state_step) //*** FT-change-2: only compute if cur-step is not partially committed
			{
				AggregatorT* agg = (AggregatorT*) get_aggregator();
				if (agg != NULL)
					agg->init();
				//===================
				clearBits();
				if (wakeAll == 1)
					all_compute();
				else
					active_compute();
				vcomp_called = true;
			}
			else//global_step_num <= global_state_step
			{
				if(cpDisableVec[step_num()]) message_buffer->load_rbuf();//*** FT-change-2: load msgs
				else
				{
					load_rbuf();//v-state
					use_backup_values = true;
					cpload_compute();
					use_backup_values = false;
				}
				vcomp_called = false;
			}
			MPI_Allgather(&global_state_step, 1, MPI_INT, &global_state_vec[0], 1, MPI_INT, global_world);//*** FT-change-2: sync state-step
			step_msg_num = master_sum_LL(message_buffer->get_total_msg());
			if (_my_rank == MASTER_RANK) global_msg_num += step_msg_num;
			message_buffer->sync_messages();
			//*** FT-change-2: agg_sync() is now only necessary when the last error is recovered
			if(step_num() < global_state_vec[_alive_rank])//case 1: _alive_rank bcast
			{
				if(global_agg != NULL)
				{
					if(_my_rank != _alive_rank)
					{
						FinalT & finAgg = *((FinalT*) global_agg);
						recvBcast(finAgg);
						safe_add<FinalT>(step_num(), finAgg, finalAggVec);
					}
					else sendBcast(finalAggVec[step_num()]);
				}
			}
			else agg_sync();
			if(vcomp_called) //*** FT-change-2: vcomp_called => msg should be logged
			{
				rwthread->join(); //*** FT-change-2: to log msgs locally & asynchronously
				delete rwthread; //*** FT-change-2: to log msgs locally & asynchronously
				rwthread = NULL;
			}
			message_buffer->clear_sendBuf(); //*** FT-change-2: clear sending buffer only after both msg-logging and are done
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
		hdfsFS fs = getlocalFS();
		while(hdfsExists(fs, "/tmp/ftpregel") == 0) hdfsDelete(fs, "/tmp/ftpregel"); //*** FT-change-2: delete local msg-log
		hdfsDisconnect(fs);
	}

private:
	HashT hash;
	VertexContainer vertexes;
	int active_count;

	MessageBuffer<VertexT>* message_buffer;
	Combiner<MessageT>* combiner;
	AggregatorT* aggregator;

	//------
	vector<char> vstate_before_update;

	//*** FT-change-2: mem-cached
	//[simplification note]: though elements before last checkpoint can be released to save space, we do not remove them for ease of implementation
	vector<PartialT> partialAggVec; //partialAggVec[i] = superstep i's partial agg before sync (i >= 1)
	vector<FinalT> finalAggVec; //finalAggVec[i] = async-ed agg at the end of superstep i (i >= 1)
	vector<char> partialCondVec; //paritalCondVec[i] = superstep i's cond-bitmap before sync (i >= 0)
	vector<char> cpDisableVec; //cpdisableVec[i] = superstep i's cp_disabled
	//****************************************************************************
};

#endif
