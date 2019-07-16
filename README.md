## R|API+ Connector for ZMAPI

Current implementation has only functionality for MD.

Tested with:
* R|API+ 10.4.0.0

### Building Instructions

Easiest way to build is by using docker.

* Install docker.
* R|API+ libraries are not open-source and I probably don't have the right to redistribute their binaries. Because of that reason you need to get R|API+ binaries and headers yourself from Rithmic directly and copy them to the right directories.
  * rapiplus/include:
    * RApiPlus.h
  * rapiplus/lib:
    * Copy the contents of lib/ of your R|API+ distribution here.
  * rapiplus/env:
    * Add your login credentials and parameters to rapiplus/example_env.
  * rapiplus/tls:
    * Extract the key/certificate files you received from Rithmic here.
* [optional] Configure build.sh to your liking (number of compilation cores, ports to expose etc.)
* Execute build.sh
