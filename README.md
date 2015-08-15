flow-mrnet
==========
MRNet integration of Flow engine

prerequsites
---------------------------------------
MRNet v4.1.0
Boost 1.55+ (Boost thread/chrono/timer/system)

following variables must be exported;

`#export PATH=/g/g92/uswickra/Flow/mrnet-flow/[install]/bin/histogram-multilevel:$PATH`

`#path to app executables - directory where the app resides`

`export PATH=/g/g92/uswickra/Flow/mrnet-flow/[install]/bin/histogram-multilevel:$PATH`

`#path to configuration home (topology/app properties ,etc) - by default flow install directory`

`export FLOW_HOME=/g/g92/uswickra/Flow/mrnet-flow/[install]/bin/histogram-multilevel`

`#library path to filter.so - directory where the app filter resides ` (optional)

`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/g/g92/uswickra/Flow/mrnet-flow/[install]/bin/histogram-multilevel`

`#library path to boost/mrnet` (optional)

`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/tools/boost-1.57.0/lib`
`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/tools/mrnet-4.1.0/lib`


How to build/run with MRNet->
---------------------------------------
using cmake build:

	mkdir build && cd build
	cmake -DCMAKE_C_COMPILER=`which gcc` -DCMAKE_CXX_COMPILER=`which g++` -DMRNET_INSTALL_DIR=[path to mrnet install dir] -DBOOST_ROOT=[path to boost install dir] -DCMAKE_INSTALL_PREFIX=../install ../ 
	make && make install
	

Final install directory structure would look like following :

|-- bin
|   |-- dataTest
|   |-- demo_backend
|   |-- demo_front
|   |-- histogram
|   |   |-- backend
|   |   |-- front
|   |   `-- libfilter.so
|   |-- histogram-multilevel
|   |   |-- backend
|   |   |-- front
|   |   `-- libfilter.so
|   `-- simple_topgen
|-- include
|   |-- data.h
|   |-- filter_init.h
|   |-- mrnet_flow.h
|   |-- mrnet_operator.h
|   |-- operator.h
|   |-- process.h
|   |-- schema.h
|   |-- sight_common_internal.h
|   `-- utils.h
`-- lib
    |-- libdemo_filter.so
    `-- libflow.a


we have maintained a Makefile based build as well (depricated)

	make clean [target]    

target can be any one of 'mrnop', 'dataTest' , 'histogram-ml' , 'histogram' 


How to run->
---------------------------------------

a) build the project, modify $REPO_PATH and $BOOST_INSTALL_DIR 
   in Makefile to include your MRNet and Boost installation directories
    
   --Notes-- 
   
    1. if boost/mrnet is in your System Path then REPO_PATH or
       BOOST_INSTALL_DIR shall be /usr/local
       
    2. you may want to add $BOOST_INSTALL_DIR/lib and/or MRNet
       $REPO_PATH to linkers path variable ie:- LD_LIBRARY_PATH


b) modify MRNet topology file to suite your configuration, an
   3 leaf node configuration is the default setting. Topology 
   file ${Flow_Dir}/top_file
           
c) once project is built simply execute it in [install]/bin/[app] directory (ie:- for histogram ==> [install]/bin/histogram-ml )

   `$ ./front`
   OR
   `$ [install]/bin/histogram-multilevel/front`

d) A successfull execution (of histogram application) shows similar output to following..



`[Reading application configuration from : /g/g92/uswickra/Flow/mrnet-flow/app.properties completed.. !`

`[FE]: Application param initialization done. sync interval : 2`

`[Reading application configuration from : /g/g92/uswickra/Flow/mrnet-flow/app.properties completed.. !`

`[BE]: Application param initialization done. numItems/Rec : 10  min_value : 10 max_value : 150  genration iterations : 5`

`histogram_range_start : 10.000000  histogram_range_stop : 150.000000 histogram_col_width : 10.000000  sync interval : 2 `

`[Reading application configuration from : /g/g92/uswickra/Flow/mrnet-flow/app.properties completed.. !`

`[Reading application configuration from : /g/g92/uswickra/Flow/mrnet-flow/app.properties completed.. !`

`[BE]: Application param initialization done. numItems/Rec : 10  min_value : 10 max_value : 150  genration iterations : 5`

`histogram_range_start : 10.000000  histogram_range_stop : 150.000000 histogram_col_width : 10.000000  sync interval : 2 `

`[BE]: Application param initialization done. numItems/Rec : 10  min_value : 10 max_value : 150  genration iterations : 5`

`histogram_range_start : 10.000000  histogram_range_stop : 150.000000 histogram_col_width : 10.000000  sync interval : 2 `

`[BE]: initialization complete PID : 3039 thread ID : 46912525268320  `

`[BE]: initialization complete PID : 3038 thread ID : 46912525268320  `

`[BE]: initialization complete PID : 3040 thread ID : 46912525268320  `

`[FE]: initialization complete PID : 3036 thread ID : 46912525268320 `

`[Reading application configuration from : /g/g92/uswickra/Flow/mrnet-flow/app.properties completed.. !`

`[Filter]: Application param initialization done. sync interval : 2 children : 3 `

`[Filter]: initialization complete now. PID : 3036 thread ID : 46912992450304 `

`[FE]: Recieved EXIT tag from MRNet Stream, moving into final phase..`

`[BE]: Backend completed... `

`[BE]: Backend completed... `

`[BE]: Backend completed... `


` Total CPU_FLOW time : 0.010000 secs`

[`FE]: Flow data transfer is successfull... !! PID : 3036 `



`CPU time : 0.020000 secs`

`Total elapsed time : 1.275000 secs`

`Sink data:`

`-------------------------------------------`

`0: [Histogram: `

`    Min: [Scalar: val={10}, type=double]: `

`    Max: [Scalar: val={150}, type=double]: `

`    Coloumn: [Scalar: val={10}, type=double]: `

`        [HistogramBin: `

`    start: [Scalar: val={10}, type=double]`

`    end: [Scalar: val={20}, type=double]`

`    count: [Scalar: val={0}, type=int]`

`]`

`    Coloumn: [Scalar: val={20}, type=double]:` 

`        [HistogramBin: `

`    start: [Scalar: val={20}, type=double]`

`    end: [Scalar: val={30}, type=double]`

`    count: [Scalar: val={30}, type=int]`


....
...
..





