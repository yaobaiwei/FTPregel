// For undirected graph

#include "basic/pregel-dev.h"
#include "utils/type.h"

using namespace std;

//input line format: vid \t numNbs nb1 nb2 ...
//output line format: vid \t numNbs nb1 degNb1 nb2 degNb2 ...

struct FieldValue_pregel {
    int field;
    vector<intpair> degNbs;
};

ibinstream& operator<<(ibinstream& m, const FieldValue_pregel& v)
{
    m << v.field;
    m << v.degNbs;
    return m;
}

obinstream& operator>>(obinstream& m, FieldValue_pregel& v)
{
    m >> v.field;
    m >> v.degNbs;
    return m;
}

//====================================

class FieldVertex_pregel : public Vertex<VertexID, FieldValue_pregel, intpair> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1) {
            vector<intpair>& nbs = value().degNbs;
            for (int i = 0; i < nbs.size(); i++) {
                send_message(nbs[i].v1, intpair(id, nbs.size()));
            }
            nbs.clear();
        } else {
            vector<intpair>& degNbs = value().degNbs;
            for (int i = 0; i < messages.size(); i++) {
                degNbs.push_back(messages[i]);
            }
            vote_to_halt();
        }
    }
};

class FieldWorker_pregel : public Worker<FieldVertex_pregel> {
    char buf[100];

public:
    //C version
    virtual FieldVertex_pregel* toVertex(char* line) {
        char* pch;
        pch = strtok(line, "\t");
        FieldVertex_pregel* v = new FieldVertex_pregel;
        v->id = atoi(pch);
        v->value().field = v->id; //set field of v as id(v)
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            int vid = atoi(pch);
            v->value().degNbs.push_back(intpair(vid, -1));
        }
        return v;
    }

    virtual void toline(FieldVertex_pregel* v, BufferedWriter& writer) {
        sprintf(buf, "%d\t%d", v->id, v->value().degNbs.size());
        writer.write(buf);
        vector<intpair>& degNbs = v->value().degNbs;
        for (int i = 0; i < degNbs.size(); i++) {
            sprintf(buf, " %d %d", degNbs[i].v1, degNbs[i].v2);
            writer.write(buf);
        }
        writer.write("\n");
    }
};

void work(string input_path, string output_path) {
    WorkerParams param;
    param.input_path = input_path;
    param.output_path = output_path;
    param.force_write = true;
    param.native_dispatcher = false;
    FieldWorker_pregel worker;
    worker.run(param);
}
int main(int argc, char ** argv)
{
	init_workers(&argc, &argv);
    work(argv[1], argv[2]);
	worker_finalize();
    return 0;
}
