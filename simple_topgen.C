#include <sstream>
#include <fstream>
#include <cstdlib>
#include <cstdio>

char *write_topology_file(char **hostnames, int comm_nodes, int be_nodes, const char *out_file);

using namespace std;

char topfile[1000];
int main(int argc, char** argv){
	if(argc < 4){
		printf("Usage: simple_topgen [host_file] [num_hosts] [output file] \n");
		return 1;
	}
	char* host_file_n = argv[1];
	char* out_file = argv[3];
	int num_hosts = atoi(argv[2]);
	//FILE *host_file = fopen(host_file_n, "r");
	ifstream input(host_file_n);
	char **hostnames;
	hostnames = (char**) malloc(sizeof(char*)*num_hosts);
	for(int j = 0 ; j < num_hosts ; j++){
		hostnames[j] = (char*) malloc(200);
	}
	int i = 0;
	char hosts[num_hosts];
	for(string host ; getline(input, host);){
		size_t len = host.copy(hostnames[i++], 200, 0);
		hostnames[i-1][len] = '\0';
	}

	write_topology_file(hostnames, 0, num_hosts - 1, out_file );
	
	//cleanup
	for(int j = 0 ; j < num_hosts ; j++){
                delete hostnames[j];
        }
	delete hostnames;
return 0;
}



char *write_topology_file(char **hostnames, int comm_nodes, int be_nodes, const char *out_file) {
    snprintf(topfile, sizeof(topfile), "%s", out_file);
    
    //printf("topfile : %s - %s -  %s -  %s\n", topfile, hostnames[0], hostnames[1], hostnames[2]);
    
    //handle root
    int i = 0, k = 0;
    //if no comm nodes there are just 2 levels
    //write root node and be nodes
    if (comm_nodes == 0) {
        FILE *top_File = fopen(topfile, "w");
        fprintf(top_File, "%s:%d => \n", hostnames[0], 0);
   	for (i = 1; i <= be_nodes ; i++) {
	    char c = ' ';	
	    if(i == be_nodes){
		c = ';';	
            }
            fprintf(top_File, "\t%s:%d %c\n", hostnames[i], i, c);
        }
	
        fflush(top_File);
        fclose(top_File);
    } else {
        FILE *top_File = fopen(topfile, "w");
        fprintf(top_File, "%s:%d => \n", hostnames[0], 0);

        //for each comm_node include in topology file
        for (i = 1; i <= comm_nodes; i++) {
            fprintf(top_File, "\t%s:%d \n", hostnames[i], i);
        }
        fflush(top_File);
        fclose(top_File);
    }

    return topfile;
}







