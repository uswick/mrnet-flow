#include<iostream>
#include<fstream>
#include "mrnet_flow.h"

int get_num_streams(){
    FILE* fp = fopen("top_file","r");

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




