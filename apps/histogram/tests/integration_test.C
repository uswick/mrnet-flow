#include "integration_test.h"

/****************************
 * Test_Op Class API
 * **************************/
void start_MRNet_Flow()
{
	/* code */
	//create MRNet Front Operator chain
	// FE Source -> TestFEOp
	//1. FE Source Operator
	unsigned int ID = 0 ;
	MRNetFESourceOperator source(0, 1, ID, "top_f", "test_backend", "test_filter.so", get_Test_Schema());
        //2. Front Op
	ID++;
	OperatorPtr frontOp = get_Test_FrontOp(ID);

	//create Flow Streams to connect Source --> Sink
        vector<SchemaPtr> outStreamSchemas = source.inConnectionsComplete();
	StreamPtr stream1 = makePtr<Stream>(*outStreamSchemas.begin());
	assert(source.getNumOutputs() == 1);
        source.outConnect(0, stream1);
	
	assert(frontOp->getNumInputs() == 1);
	frontOp->inConnect(0, stream1);

	//start work
	source.driver();

}

/****************************
 * Test_Op Class API
 * **************************/
Test_Op::Test_Op (unsigned int id): Operator(1, 1, id){

}

Test_Op:: ~Test_Op (){

}

void Test_Op::recv(unsigned int inStreamIdx, DataPtr obj){
	printf("[Test] Operator :%d data recv successfull on %u  \n", ID, inStreamIdx);
	outStreams[0]->transfer(obj);
}

std::vector<SchemaPtr> Test_Op::inConnectionsComplete() {
	// The schemas of all the outgoing streams are the same as the schema of the incoming one
	vector<SchemaPtr> ret;
	assert(inStreams.size()==1);
	for(int i=0; i<numOutputs; ++i)
		ret.push_back(inStreams[0]->getSchema());
	return ret;
}


/****************************
 * Test_CallbackOp Class API
 * **************************/

Test_CallbackOp::Test_CallbackOp (unsigned int id, callback_func cb): Test_Op(id){
	call_b = cb ;     
}

void Test_CallbackOp::recv(unsigned int inStreamIdx, DataPtr obj){
	printf("[Test] Operator :%d data recv successfull on %u  \n", ID, inStreamIdx);
	(*call_b)(inStreamIdx, obj);
	assert(outStreams.size()==1);
	outStreams[0]->transfer(obj);
}

/****************************
 * Test_CallbackOp Class API
 * **************************/

Test_SourceOp::Test_SourceOp(unsigned int id, src_callback_func scb): SourceOperator(1, 1, id){
	src_f = scb;
}

Test_SourceOp::~Test_SourceOp (){
}

std::vector<SchemaPtr> Test_SourceOp::inConnectionsComplete() {
	vector<SchemaPtr> ret;
	ret.push_back(get_Test_Schema());
	return ret;
}

void Test_SourceOp::work(){
	printf("[Test] Source Operator :%d entered on work()  \n", ID);
	//do some work returned data using callback 'src_f'
	vector<DataPtr> data = (*src_f)();
	//iterate over data set and trasnfer each record to next operator in chain
	vector<DataPtr>::iterator it = data.begin();
        for (; it != data.end(); it++) {
        	/* code */
	     outStreams[0]->transfer(*it);
        }
}

