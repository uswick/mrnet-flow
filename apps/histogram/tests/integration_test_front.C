#include <stdio.h>
#include "integration_test.h"


OperatorPtr get_Test_FrontOp(unsigned int ID){
       OperatorPtr fOp = makePtr<Test_Op>(ID);
       return fOp;       
}

int main(int argc, const char *argv[])
{
        //register tests
	//run tests with MRNet 
	printf("aaa");
	return 0;
}
