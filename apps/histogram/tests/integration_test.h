#include "flow_test.h"

typedef void (*callback_func)(int, DataPtr);

typedef vector<DataPtr> (*src_callback_func)();

class Test_Op :public Operator{
	private:
		/* data */
	public:
		Test_Op (unsigned int id) ;

		virtual ~Test_Op ();

		virtual void recv(unsigned int inStreamIdx, DataPtr obj);

		std::vector<SchemaPtr> inConnectionsComplete();
};


class Test_CallbackOp :public Test_Op{
	private:
		callback_func call_b;
		/* data */
	public:
		Test_CallbackOp (unsigned int id, callback_func cb);
		void recv(unsigned int inStreamIdx, DataPtr obj);

};		

class Test_SourceOp : public SourceOperator {
public:
	Test_SourceOp (unsigned int id, src_callback_func scb);
	virtual ~Test_SourceOp ();
        void work();
	std::vector<SchemaPtr> inConnectionsComplete();

private:
	src_callback_func src_f;/* data */
};
/*******************************************************************************
 * This method will Register a BE source operator with Integration Test 
 * framework, that will generate some input.
 *    return - some opeartor to generate backend Input
 *
 * ***********************************************************/
OperatorPtr get_Test_BE_SourceOp(unsigned int ID);

/*******************************************************************************
 * This method will Register a Filter operator  that will register an operator
 * in the operator chain on Integration Test framework as for filter nodes. 
 * Specifically operator is registered after FilterSource but before FilterSink
 *    return - some opeartor to test filter nodes
 *
 * ***********************************************************/
OperatorPtr get_Test_FilterOp(unsigned int ID);

/*******************************************************************************
 * This method will Register a Front end operator  that will register an operator
 * in the operator chain on Integration Test framework as for front end node. 
 * Specifically operator is registered  after FrontSource but before FrontSink
 *    return - some opeartor to test front node operation
 *
 * ***********************************************************/
OperatorPtr get_Test_FrontOp(unsigned int ID);


/*******************************************************************************
 * This method will Register a Common Inter node Schema that will be common
 * between BE , Filter and FE.   
 * return - some common Internode schema
 *
 * ***********************************************************/
SchemaPtr get_Test_Schema();


/*******************************************************************************
 * This method will start MRNet Flow before integration tests are executed.
 *
 * ***********************************************************/
void start_MRNet_Flow();
