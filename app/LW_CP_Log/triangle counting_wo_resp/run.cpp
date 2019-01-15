#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "signal.h"
#define C 10

struct TCValue_pregel {
    // number of triangles
    int count;
    int send_count;
    int deg;

    // iterator position
    size_t pos_i;
    size_t pos_j;
};

ibinstream & operator<<(ibinstream & m, const TCValue_pregel & v) {
    m<<v.count;
    m<<v.send_count;
    m<<v.deg;
    m<<v.pos_i;
    m<<v.pos_j;
    return m;
}

obinstream & operator>>(obinstream & m, TCValue_pregel & v) {
    m>>v.count;
    m>>v.send_count;
    m>>v.deg;
    m>>v.pos_i;
    m>>v.pos_j;
    return m;
}

typedef Edge<VertexID, int> TCEdge;

bool operator<(const TCEdge& lhs, const TCEdge& rhs)
{
	return (lhs.eval < rhs.eval) || ((lhs.eval == rhs.eval) && (lhs.id < rhs.id));
}

bool operator==(const TCEdge& lhs, const TCEdge& rhs)
{
	return (lhs.eval == rhs.eval) && (lhs.id == rhs.id);
}

class TCVertex_pregel : public EVertex<VertexID, TCValue_pregel, int, TCEdge> {
public:
    virtual void compute(MessageContainer & messages) {
    	//*** FT-change
		//*
		if(!newly_respawned)//only kill for the first time
		{
			if(_my_rank == 1 && step_num()==17)
			{
				printf("%d: I am killing myself !!!\n", _my_rank);
				raise(SIGKILL);
				while(1);
			}
		}
		//*/
    	vector<TCEdge> & nbs = edges();
        size_t numPendingMsg = C*value().deg;//quota of requests
        int num_msgs = 0;
        if(step_num() % 2 == 1) {
        	//odd, request
        	//================== part 1: <V_old> -> <V_new>
        	//forwarding
        	size_t i = value().pos_i;
        	size_t j = value().pos_j;
        	size_t len = nbs.size();
        	j++;//forward to next (i, j)
        	for(; i<len; i++)
			{
				for(; j<len; j++)
				{
					num_msgs++;
					numPendingMsg--;
					if(numPendingMsg == 0) goto tag;
				}
				j = i+2;//next i is (i++), and next j should be one more
			}
        tag:
        	//update value()
        	value().pos_i = i;
        	value().pos_j = j;
        	value().send_count = num_msgs;
        	//================== part 2: <V_new> -> msgs
        	//get value() again
        	i = value().pos_i;
			j = value().pos_j;
			num_msgs = value().send_count;
        	//generate msgs from value()
        	if(j>=len)
        	{
        		j=len-1;
        		i=j-1;
        	}
        	for(int k=0; k<num_msgs; k++)
        	{
        		send_message(nbs[i].id, nbs[j]);
        		j--;
        		if(j<=i)
        		{
        			i--;
        			j=len-1;
        		}
        	}
        	if(num_msgs == 0) vote_to_halt();
        }
        else
        {
            //even, respond
        	//cp_disabled = true;//!!! IMPORTANT !!! disable checkpointing //but no need here, as nothing is responded
        	vector<TCEdge> & nbs = edges();
            for(int i=0; i<messages.size(); i++) {
            	bool found = binary_search(nbs.begin(), nbs.end(), messages[i]);
                if(found) value().count++;
            }
        }
    }
};

//input line format:
//vid num_nbs \t nb1 deg1 nb2 deg2 ...

class TCWorker_pregel : public Worker<TCVertex_pregel> {
	char buf[100];
public:
    virtual TCVertex_pregel * toVertex(char * line) {
        char * pch;
        pch = strtok(line, "\t");
        TCVertex_pregel * v = new TCVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().deg = atoi(pch);
        TCEdge me;
        me.id = v->id;
        me.eval = v->value().deg;
        vector<TCEdge> & nbs = v->edges();
        for(int i=0; i<me.eval; i++) {
            pch = strtok(NULL, " ");
            TCEdge edge;
            edge.id = atoi(pch);
            pch = strtok(NULL, " ");
            edge.eval = atoi(pch);
            if(me < edge) nbs.push_back(edge);
        }
        sort(nbs.begin(), nbs.end());
        v->value().count = 0;
        v->value().pos_i = 0;
        v->value().pos_j = 0;
        //now is (0, 0), so that next is (0, 1)
        return v;
    }
    virtual void toline(TCVertex_pregel * v, BufferedWriter & writer) {
        sprintf(buf, "%d\t%d\n", v->id, v->value().count);
        writer.write(buf);
    }
};

void pregel_triangle(string input_path, string output_path) {
    WorkerParams param;
    param.input_path = input_path;
    param.output_path = output_path;
    param.force_write = true;
    param.native_dispatcher = false;
    TCWorker_pregel worker;
    worker.setCPGap(5); //###### set CP period here
    worker.run(param);
}

int main(int argc, char ** argv) {
	init_workers(&argc, &argv);
	pregel_triangle("/pullgel/physics_deg", "/toyOutput");
	worker_finalize();
    return 0;
}
