#ifndef MESSAGEBUFFER_H
#define MESSAGEBUFFER_H

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
#include <sys/stat.h> //*** FT-change-2: to support local save
#include <fstream> //*** FT-change-2: to support local save
using namespace std;

void _mkdir(const char *dir) {//taken from: http://nion.modprobe.de/blog/archives/357-Recursive-directory-creation.html
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for(p = tmp + 1; *p; p++)
		if(*p == '/') {
				*p = 0;
				mkdir(tmp, S_IRWXU);
				*p = '/';
		}
	mkdir(tmp, S_IRWXU);
}

template <class VertexT>
class MessageBuffer {
public:
    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename VertexT::HashType HashT;
    typedef vector<MessageT> MessageContainerT;
    typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
    typedef Vecs<KeyT, MessageT, HashT> VecsT;
    typedef typename VecsT::Vec Vec;
    typedef typename VecsT::VecGroup VecGroup;
    typedef typename Map::iterator MapIter;

    VecsT out_messages;
    Map in_messages;
    vector<MessageContainerT> v_msg_bufs;
    HashT hash;

    void init(vector<VertexT*> & vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }
    void reinit(vector<VertexT*> & vertexes)
    {
        v_msg_bufs.resize(vertexes.size());
        in_messages.clear();
        for (int i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            in_messages[v->id] = i; //CHANGED FOR VADD
        }
    }

    //*** FT-change: newly added to emptize the msgbuf a normal process (respawned process just call init())
	void clear_msgs()
	{
		out_messages.clear();
		for(int i=0; i<v_msg_bufs.size(); i++) v_msg_bufs[i].clear();
	}

    void add_message(const KeyT& id, const MessageT& msg)
    {
        hasMsg(); //cannot end yet even every vertex halts
        out_messages.append(id, msg);
    }

    Map& get_messages()
    {
        return in_messages;
    }

    void combine()
    {
        //apply combiner
        Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
        if (combiner != NULL)
            out_messages.combine();
    }

    void sync_messages()
    {
    	int np = get_num_workers();
        int me = get_worker_id();
        //exchange msgs
        VecGroup to_recv(np);
        all_to_all(out_messages.getBufs(), to_recv);
        // gather all messages
        for (int i = 0; i < np; i++) {
			if(i == me)
			{
				Vec& msgBuf = out_messages.getBufs()[i];
				for (int j = 0; j < msgBuf.size(); j++) {
					MapIter it = in_messages.find(msgBuf[j].key);
					if (it != in_messages.end()) //filter out msgs to non-existent vertices
						v_msg_bufs[it->second].push_back(msgBuf[j].msg); //CHANGED FOR VADD
				}
			}
			else
			{
				Vec& msgBuf = to_recv[i];
				for (int j = 0; j < msgBuf.size(); j++) {
					MapIter it = in_messages.find(msgBuf[j].key);
					if (it != in_messages.end()) //filter out msgs to non-existent vertices
						v_msg_bufs[it->second].push_back(msgBuf[j].msg); //CHANGED FOR VADD
				}
			}
		}
    }

    void clear_sendBuf()
	{
		out_messages.clear();
	}

    //*** FT-change-2: to save out_messages to local disk location: /tmp/ftpregel/#superstep/(_my_rank)_(tgt_rank)
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
		VecGroup & msgbufs = out_messages.getBufs();
		for(int i=0; i<_num_workers; i++)
		{
			Vec & mbuf = msgbufs[i];
			//---
			sprintf(numbuf, "%d", i);
			string file_path = save_path + "_" + numbuf;
			ofstream out(file_path.c_str());
			ibinstream rbuf;
			rbuf << out_messages.getBufs()[i];
			out.write(rbuf.get_buf(), rbuf.size());
			out.close();
		}
		if(newly_respawned)
		{
			StopTimer(IO_TIMER);
			cout << _my_rank << "'s msg-logging time: " << get_timer(IO_TIMER) << endl;
		}
	}

	//*** FT-change-2: to load out_messages from local disk location: /tmp/ftpregel/#superstep/_my_rank
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
		for(int i=0; i<_num_workers; i++)
		{
			if(step_num() < global_state_vec[i]) continue; //*** FT-change-2: do not load
			sprintf(numbuf, "%d", i);
			string file_path = save_path + "_" + numbuf;
			//get file size
			struct stat statbuf;
			stat(file_path.c_str(), &statbuf);
			int fsize = statbuf.st_size;
			char* fbuf = new char[fsize];
			//read msgs
			ifstream in(file_path.c_str());
			in.read(fbuf, fsize);
			in.close();
			//parse to sendBuf
			obinstream oMsgStream(fbuf, fsize);
			oMsgStream >> out_messages.getBufs()[i];
		}
	}

    long long get_total_msg()
    {
        return out_messages.get_total_msg();
    }

    vector<MessageContainerT>& get_v_msg_bufs()
    {
        return v_msg_bufs;
    }
};

#endif
