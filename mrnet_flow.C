#include<iostream>
#include<fstream>
#include <cstdlib>
#include "mrnet_flow.h"

int get_num_streams(){
    const char* env_flowp = std::getenv("FLOW_HOME");
    char top_file[1000];
    if(env_flowp == NULL){
	std::snprintf(top_file,1000,"top_file");
    }else{
    	std::snprintf(top_file,1000,"%s/top_file",env_flowp);
    }
    //FILE* fp = fopen("/N/u/uswickra/Karst/Flow/mrnet-flow/top_file","r");
    //FILE* fp = fopen("top_file","r");
    FILE* fp = fopen(top_file,"r");

    char ch ;
    int count = 0;
    typedef enum{
        S, PARSE, END
    } state;

    state st = S ; 
    //parser untill we see terminate symbol
    while(!feof(fp)){
        ch = getc(fp);
        if(ch == '>'){
            st = PARSE;       
        }else if(ch == ';'){
            st = END ;
        }

        if(st == PARSE && ch == ':'){
            count++;
        }else if(st == END){
            break;
        }
    };
    fclose(fp);

    return count ;
}




