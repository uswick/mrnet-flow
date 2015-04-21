#include<iostream>
#include<fstream>
#include <cstdlib>
#include "mrnet_flow.h"
#include "filter_init.h"
#include "unistd.h"


const char* BE_filter = "defaultFlowFilter";
const char* FE_filter = "defaultFlowFilter";
const char* CP_filter = "CP_FlowFilter";

glst_t nullFilterInfo;
static std::map<long, std::map<string, void*> > thread_storage;

char* get_configFE(){
    return (char*)"opconfig_frontend";
}

char* get_configBE(){
    long th_id = (long)pthread_self();
    if(thread_storage[th_id].find("backend_config_file") == thread_storage[th_id].end()){
        char* opConfigFName = (char*) malloc(sizeof(char) * 200);
        snprintf(opConfigFName, 200, "opconfig_backend.%d.%lu",getpid(),th_id);
        (thread_storage[th_id])["backend_config_file"] = opConfigFName;
    }
    return  (char*)(thread_storage[th_id])["backend_config_file"];
}

char* get_configFilter(){
    long th_id = (long)pthread_self();
    if(thread_storage[th_id].find("filter_config_file") == thread_storage[th_id].end()){
        char* opConfigFName = (char*) malloc(sizeof(char) * 200);
        snprintf(opConfigFName, 200, "opconfig_filter.%d.%lu",getpid(),th_id);
        (thread_storage[th_id])["filter_config_file"] = opConfigFName;
    }
    return  (char*)(thread_storage[th_id])["filter_config_file"];
}

char* get_Source(){
    long th_id = (long)pthread_self();
    if(thread_storage[th_id].find("input_source_file") == thread_storage[th_id].end()){
        char*sourceFName = (char*) malloc(sizeof(char) * 200);
        snprintf(sourceFName, 200, "source.%d.%lu",getpid(),pthread_self());
        (thread_storage[th_id])["input_source_file"] = sourceFName;
    }
    return  (char*)(thread_storage[th_id])["input_source_file"];
}

char* get_Sink(){
    return (char*)"sink";
}

int __thread BE_ARG_CNT = 0 ;
char**  BE_ARGS = NULL ;

void Flow_Init(int argc, char** argv){
    //do BE specific init
    BE_ARG_CNT = argc ;
    BE_ARGS = argv ;

    //init storage map
    long th_id = (long)pthread_self();
    std::map<string, void*> config_map;
    thread_storage[th_id] = config_map;


}

void Flow_Finalize(){
    //cleanup files
    std::remove(CONFIG_FE);
    std::remove(CONFIG_BE);
    std::remove(CONFIG_FILTER);
    std::remove(SOURCE_FILE);
    
    //cleanup map
    long th_id = (long)pthread_self();
    std::map<string, void*>::iterator itemsIt =  thread_storage[th_id].begin();

    for( ; itemsIt != thread_storage[th_id].end() ; itemsIt++ ){
        void* allocated = itemsIt->second;
        free(allocated);
    }

}


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

// Reads a given file using a given Schema and prints the Data objects in it
void printDataFile(const char* fName, SchemaPtr schema) {
    FILE* in = fopen(fName, "r");
    assert(in);
    unsigned int dNum=0;
    fgetc(in);
    while(!feof(in)) {
        fseek(in, -1, SEEK_CUR);
        DataPtr data = schema->deserialize(in);
        cout << dNum << ": "; data->str(cout, schema) << endl;
        cout << "-----"<<endl;
        ++dNum;
        fgetc(in);
    }

    fclose(in);
}



// Given a parser that reads a given configuration file, load it and run it.
// Returns a mapping of the IDs of sink operators (special types that produce externally-visible
// artifacts like files or sockets) to the schemas of their outputs.
SharedPtr<SourceOperator> getFlowSource(structureParser& parser, glst_t& filter_info) {
    // Maps Operator unique IDs to pointers to the Operators themselves
    map<unsigned int, OperatorPtr> operators;

    // Operators that have no incoming edges. Initially filled with all operators.
    // Then we remove all operators that have an incoming edge.
    set<unsigned int> sourceOps;

    // Operators that have no outgoing edges. Initially filled with all operators.
    // Then we remove all operators that have an outgoing edge.
    set<unsigned int> sinkOps;

    // Load all the operators
    {
        propertiesPtr allOpTags = parser.nextFull();
        assert(allOpTags->name() == "Operators");

        for(list<propertiesPtr>::const_iterator opTag=allOpTags->getContents().begin(); opTag!=allOpTags->getContents().end(); ++opTag) {
            // Deserialize this Operator and add it to operators
            OperatorPtr newOp = OperatorRegistry::create(*opTag);
            if(operators.find(newOp->getID()) != operators.end())
            { cerr << "ERROR: operator ID "<<newOp->getID()<<" has previously been observed!"<<endl;
                cerr << "Prior operator: "; operators[newOp->getID()]->str(cerr); cerr<<endl;
                cerr << "New operator: ";   newOp->str(cerr);                     cerr<<endl;
                assert(0); }
            operators[newOp->getID()] = newOp;
            sourceOps.insert(newOp->getID());
            sinkOps.insert(newOp->getID());
        }
    }

    /*cout << "operators:"<<endl;
    for(map<unsigned int, OperatorPtr>::iterator op=operators.begin(); op!=operators.end(); ++op) {
      cout << "    "<<op->first<<": "; op->second->str(cout); cout << endl;
    }*/

    // Maps each operator to its outgoing and incoming edges
    map<unsigned int, list<Op2OpEdge> > outEdges;
    map<unsigned int, vector<StreamPtr> > inEdges;

    // Initialize inEdges to assign enough storage to each vector to hold all the
    // inputs of its corresponding Operator
    for(map<unsigned int, OperatorPtr>::iterator op=operators.begin(); op!=operators.end(); ++op)
        if(op->second->getNumInputs()>0)
            inEdges[op->first].resize(op->second->getNumInputs());

    {
        propertiesPtr allStreamTags = parser.nextFull();
        assert(allStreamTags->name() == "Streams");

        // Load all the operator->operator streams
        for(list<propertiesPtr>::const_iterator streamTag=allStreamTags->getContents().begin(); streamTag!=allStreamTags->getContents().end(); ++streamTag) {
            assert(operators.find((*streamTag)->begin().getInt("fromOpID")) != operators.end());
            assert(operators.find((*streamTag)->begin().getInt("toOpID")) != operators.end());

            // Deserialize this edge and add it to outEdges and inEdges
            Op2OpEdge edge((*streamTag)->begin().getInt("fromOpID"), (*streamTag)->begin().getInt("fromOpPort"),
                    (*streamTag)->begin().getInt("toOpID"),   (*streamTag)->begin().getInt("toOpPort"));
            outEdges[edge.fromOpID].push_back(edge);
            /* // Add a NULL Stream Pointer to inEdges to ensure that the vector has space once we place the real StreamPtr in it
            unsigned int toOpPort = (*streamTag)->begin().getInt("toOpPort");
            inEdges[edge.toOpID].reserve(toOpPort);
            inEdges[edge.toOpID][toOpPort] = NULLStream;*/

            // Remove the edge's target Operator from sourceOps
            sourceOps.erase((unsigned int)((*streamTag)->begin().getInt("toOpID")));

            // Remove the edge's source Operator from sinkOps
            sinkOps.erase((unsigned int)((*streamTag)->begin().getInt("fromOpID")));
        }
    }


    // Connect Operators via Streams
    for(map<unsigned int, OperatorPtr>::iterator opIt=operators.begin(); opIt!=operators.end(); ++opIt) {
        unsigned int opID = opIt->first;
        OperatorPtr op = opIt->second;

        // If this operator has any incoming streams
        if(op->getNumInputs()>0) {
            // Connect the incoming streams to op's input ports
            map<unsigned int, vector<StreamPtr> >::iterator in=inEdges.find(opID);
            assert(in != inEdges.end());
            unsigned int inPort=0;
            for(vector<StreamPtr>::iterator ie=in->second.begin(); ie!=in->second.end(); ++ie, ++inPort) {
                op->inConnect(inPort, *ie);
            }

            // Else, if this operator has no incoming streams
        } else
            op->inConnectionsComplete();

        vector<SchemaPtr> outStreamSchemas = op->inConnectionsComplete();
//        printf("OP ID : %d num outputs : %d out schemas : %d \n",op->getID(), op->getNumOutputs(), outStreamSchemas.size());
        assert(op->getNumOutputs() == outStreamSchemas.size());
        if(op->getNumOutputs()>0) {
            map<unsigned int, list<Op2OpEdge> >::iterator out=outEdges.find(opID);
            assert(out != outEdges.end());

            // Create Streams for all the outgoing ports, connect them to the op's output ports and record them for later connection
            unsigned int outPort=0;
            assert(out->second.size() == outStreamSchemas.size());
            list<Op2OpEdge>::iterator oe=out->second.begin();
            vector<SchemaPtr>::iterator os=outStreamSchemas.begin();
            for(; os!=outStreamSchemas.end(); ++oe, ++os, ++outPort) {
                StreamPtr outStream = makePtr<Stream>(*os);
                op->outConnect(outPort, outStream);
                cout << "inEdges[oe->toOpID].size()="<<inEdges[oe->toOpID].size()<<", oe->toOpPort="<<oe->toOpPort<<" , oe->toOpID=" << oe->toOpID <<
				 ", oe->fromOpPort"<< oe->fromOpPort <<  ", oe->fromOpID" << oe->fromOpID  <<endl;
                assert(inEdges[oe->toOpID].size() > oe->toOpPort);
                inEdges[oe->toOpID][oe->toOpPort] = outStream;
            }
        }

        op->outConnectionsComplete();
    }

    if(sourceOps.size()==0) { cerr << "ERROR: no source operators! Need some operator to produce data."<<endl; assert(0); }
    if(sourceOps.size()>1) { cerr << "ERROR: multiple source operators not supported in sequential implementation!"<<endl; assert(0); }
    assert(operators.find(*sourceOps.begin()) != operators.end());

    SharedPtr<SourceOperator> source_op = dynamicPtrCast<SourceOperator>(operators[*sourceOps.begin()]);

    //check if a null object is passed for Filter info
    if (&filter_info != &nullFilterInfo) {
        filter_info.op = source_op;
//    source_op = dynamicPtrCast<SourceOperator>(operators[*sourceOps.begin()]);
        if (!source_op) {
            cerr << "ERROR: source operator does not derive from SourceOperator! operator=";
            operators[*sourceOps.begin()]->str(cerr);
            cerr << endl;
            assert(0);
        }

        //get output schema for  debugging
        for (set<unsigned int>::iterator op = sinkOps.begin(); op != sinkOps.end(); ++op) {
//        out_op_filter = dynamicPtrCast<MRNetFilterOutOperator>(operators[*op]);
            filter_info.sink = operators[*op];
            if (filter_info.sink) {
                assert(filter_info.sink->getInStreams().size() == 1);
//            output_schemas[*op] = (*out_op_filter->getInStreams().begin())->getSchema();
                filter_info.out_schemas[*op] = (*filter_info.sink->getInStreams().begin())->getSchema();
            }
        }
    }

    return source_op;
}





// Given a parser that reads a given configuration file, load it and run it.
// Returns a mapping of the IDs of sink operators (special types that produce externally-visible
// artifacts like files or sockets) to the schemas of their outputs.
std::map<unsigned int, SchemaPtr> runFlow(structureParser& parser) {

    glst_t fltrInf;
    SharedPtr<SourceOperator> source = getFlowSource(parser, fltrInf);
    // Run the workflow, with the source emitting data and other operators receiving and propagating it
    source->driver();

    // The source has now completed and streamFinished() tokens have been propagated along all streams.

    // Collect the output schemas of the sink operators
//    map<unsigned int, SchemaPtr> outSchemas;
//    for(set<unsigned int>::iterator op=sinkOps.begin(); op!=sinkOps.end(); ++op) {
//        OutFileOperatorPtr outFile = dynamicPtrCast<OutFileOperator>(operators[*op]);
//        if(outFile) {
//            assert(outFile->getInStreams().size()==1);
//            outSchemas[*op] = (*outFile->getInStreams().begin())->getSchema();
//        }
//    }

    // We now shut down by destroying all Streams and Operators.
    return fltrInf.out_schemas;
}





