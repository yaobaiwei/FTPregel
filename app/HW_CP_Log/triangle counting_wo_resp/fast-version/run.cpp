#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "signal.h"
#define C 10

struct TCValue_pregel {
    // v1 for neighbor degree, v2 for neighbor id
    vector<intpair> nbs;

    // number of triangles
    int count;
    int deg;

    // iterator position
    size_t pos_i;
    size_t pos_j;
};

ibinstream & operator<<(ibinstream & m, const TCValue_pregel & v) {
    m<<v.nbs;
    m<<v.count;
    m<<v.deg;
    m<<v.pos_i;
    m<<v.pos_j;
    return m;
}

obinstream & operator>>(obinstream & m, TCValue_pregel & v) {
    m>>v.nbs;
    m>>v.count;
    m>>v.deg;
    m>>v.pos_i;
    m>>v.pos_j;
    return m;
}

class TCVertex_pregel : public Vertex<VertexID, TCValue_pregel, intpair> {
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
    	vector<intpair> & nbs = value().nbs;
        size_t numPendingMsg = C*value().deg;//quota of requests
        if(step_num() % 2 == 1) {
            //odd, request
        	size_t & i = value().pos_i;
        	size_t & j = value().pos_j;
        	size_t len = nbs.size();
        	j++;//forward to next (i, j)
        	for(; i<len; i++)
        	{
        		for(; j<len; j++)
				{
					send_message(nbs[i].v2, nbs[j]);//j = max(i, j) since we require j > i
					numPendingMsg--;
					if(numPendingMsg == 0) return;
				}
        		j = i+2;//next i is (i++), and next j should be one more
        	}
        	vote_to_halt();
        }
        else
        {
            //even, respond
        	vector<intpair> & nbs = value().nbs;
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
        int deg = atoi(pch);
        v->value().deg = deg;
        vector<intpair> & nbs = v->value().nbs;
        intpair me(deg, v->id);
        for(int i=0; i<deg; i++) {
            pch = strtok(NULL, " ");
            int nbId = atoi(pch);
            pch = strtok(NULL, " ");
            int nbDeg = atoi(pch);
            intpair ip(nbDeg, nbId);//primary key is deg, secondary key is ID
            if(me < ip) nbs.push_back(ip);
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
