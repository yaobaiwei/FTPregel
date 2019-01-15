#ifndef MESSAGEBUFFER_H
#define MESSAGEBUFFER_H

#include <vector>
#include "../utils/global.h"
#include "../utils/Combiner.h"
#include "../utils/communication.h"
#include "../utils/vecs.h"
using namespace std;

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
        all_to_all(out_messages.getBufs());
        // gather all messages
        for (int i = 0; i < np; i++) {
			Vec& msgBuf = out_messages.getBuf(i);
			for (int j = 0; j < msgBuf.size(); j++) {
				MapIter it = in_messages.find(msgBuf[j].key);
				if (it != in_messages.end()) //filter out msgs to non-existent vertices
					v_msg_bufs[it->second].push_back(msgBuf[j].msg); //CHANGED FOR VADD
			}
		}
        //clear out-msg-buf
        out_messages.clear();
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
