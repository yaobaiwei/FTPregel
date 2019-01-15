#ifndef VERTEX_H
#define VERTEX_H

#include "../utils/global.h"
#include <vector>
#include "../utils/serialization.h"
#include "MessageBuffer.h"
using namespace std;

//Default Hash Function =====================
template <class KeyT>
class DefaultHash {
public:
    inline int operator()(KeyT key)
    {
        if (key >= 0)
            return key % _num_workers;
        else
            return (-key) % _num_workers;
    }
};
//==========================================

template <class KeyT, class ValueT, class MessageT, class HashT = DefaultHash<KeyT> >
class Vertex {
public:
    KeyT id;
    ValueT _value;
	vector<KeyT> _edges;
	bool active;

    typedef KeyT KeyType;
    typedef ValueT ValueType;
    typedef MessageT MessageType;
    typedef HashT HashType;
    typedef vector<MessageType> MessageContainer;
    typedef typename MessageContainer::iterator MessageIter;
    typedef Vertex<KeyT, ValueT, MessageT, HashT> VertexT;
    typedef MessageBuffer<VertexT> MessageBufT;

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
    	if(global_need_copy){
    		ValueT & val = *(ValueT *)global_value_copy;
			val = _value;
			return val;
		}
		else return _value;
    }

    //*** FT-change-CPlight
    inline const ValueT& value() const
    {
    	if(global_need_copy){
			ValueT & val = *(ValueT *)global_value_copy;
			val = _value;
			return val;
		}
		else return _value;
    }

    //*** FT-change-CPlight
	inline vector<KeyT>& edges()
	{
		return _edges;
	}

	//*** FT-change-CPlight
	inline const vector<KeyT>& edges() const
	{
		return _edges;
	}

    Vertex()
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
