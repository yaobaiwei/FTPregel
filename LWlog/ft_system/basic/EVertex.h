#ifndef EVERTEX_H
#define EVERTEX_H

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"
#include "MessageBuffer.h"
#include "Vertex.h"
using namespace std;

template <class KeyT, class EdgeT>
class Edge
{
public:
	KeyT id;
	EdgeT eval;

	friend ibinstream& operator<<(ibinstream& m, const Edge<KeyT, EdgeT>& v)
	{
		m << v.id;
		m << v.eval;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Edge<KeyT, EdgeT>& v)
	{
		m >> v.id;
		m >> v.eval;
		return m;
	}
};

template <class KeyT, class ValueT, class EdgeT, class MessageT, class HashT = DefaultHash<KeyT> >
class EVertex {
public:
    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef MessageT MessageType;
    typedef HashT HashType;
    typedef vector<MessageType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef EVertex<KeyT, ValueT, EdgeT, MessageT, HashT> VertexT;
    typedef Edge<KeyT, EdgeT> AdjT;
    typedef MessageBuffer<VertexT> MessageBufT;

    KeyT id;
	ValueT _value;
	vector<AdjT> _edges;
	bool active;

    friend ibinstream& operator<<(ibinstream& m, const VertexT& v)
    {
        m << v.id;
        m << v._value;
        m << v._edges;
        return m;
    }

    friend obinstream& operator>>(obinstream& m, VertexT& v)
    {
        m >> v.id;
        m >> v._value;
        m >> v._edges;
        return m;
    }

    virtual void compute(MessageContainer& messages) = 0;

    //*** FT-change-CPlight
	inline ValueT& value()
	{
		if(use_backup_values)
		{
			vector<ValueT> & vvec = *(vector<ValueT>*)global_value_vec;
			if(global_need_copy){
				ValueT & val = *(ValueT *)global_value_copy;
				val = vvec[global_vpos];
				return val;
			}
			else return vvec[global_vpos];
		}
		else
		{
			if(global_need_copy){
				ValueT & val = *(ValueT *)global_value_copy;
				val = _value;
				return val;
			}
			else return _value;
		}
	}

	//*** FT-change-CPlight
	inline const ValueT& value() const
	{
		if(use_backup_values)
		{
			vector<ValueT> & vvec = *(vector<ValueT>*)global_value_vec;
			if(global_need_copy){
				ValueT & val = *(ValueT *)global_value_copy;
				val = vvec[global_vpos];
				return val;
			}
			else return vvec[global_vpos];
		}
		else
		{
			if(global_need_copy){
				ValueT & val = *(ValueT *)global_value_copy;
				val = _value;
				return val;
			}
			else return _value;
		}
	}

    //*** FT-change-CPlight
	inline vector<AdjT>& edges()
	{
		return _edges;
	}

	//*** FT-change-CPlight
	inline const vector<AdjT>& edges() const
	{
		return _edges;
	}

    EVertex()
        : active(true)
    {
    }

    inline bool operator<(const VertexT& rhs) const
    {
        return id < rhs.id;
    }
    inline bool operator==(const VertexT& rhs) const
    {
        return id == rhs.id;
    }
    inline bool operator!=(const VertexT& rhs) const
    {
        return id != rhs.id;
    }

    inline bool is_active()
    {
        return active;
    }

    //*** FT-change-CPlight
    inline void activate()
    {
    	if(!global_need_copy) active = true;
    }

    //*** FT-change-CPlight
    inline void vote_to_halt()
    {
    	if(!global_need_copy) active = false;
    }

    void send_message(const KeyT& id, const MessageT& msg)
    {
        ((MessageBufT*)get_message_buffer())->add_message(id, msg);
    }

    void add_vertex(VertexT* v)
    {
        ((MessageBufT*)get_message_buffer())->add_vertex(v);
    }
};

#endif
