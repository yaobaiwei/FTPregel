#include "pregel_app_pagerank.h"

int main(int argc, char* argv[]){
	init_workers(&argc, &argv);
	pregel_pagerank("/pullgel/webuk", "/toyOutput", true);
	worker_finalize();
	return 0;
}
