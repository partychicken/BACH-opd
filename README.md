# BACH_demo

An open-source implementation for BACH: <u>B</u>ridging <u>A</u>djacency List and <u>C</u>SR Format using LSM-Trees for <u>H</u>GTAP Workloads.

G++12 needed.

## Building
```bash
mkdir build && cd build
cmake ..
make -j 8
```

## Structure
- `include/BACH` the header file of BACH.
- `include/dynamic_bitset` dynamic_bitset library from https://github.com/pinam45/dynamic_bitset
- `src` the source code of BACH.

## Benchmark

We used the following branches of gfe_driver for system testing which can be found in the following repository:

https://github.com/2600254/gfe_driver/

The executable files for the SNB test can be found in the Releases.
Run the following command to start SNB server:

```bash
mkdir server_log
TZ=UTC ./snb_server server_log /path/to/social_network 9090
```

## Usage in your own work

```c++
#include "include/BACH/BACH.h"
```

Alternatively, you can use dynamic library:

```c++
#include "bind/BACH.h"
```

Then add the linking options during building:
```bash
-lbach -L/path/to/BACH_demo/build 
```

<!-- oneTBB https://github.com/oneapi-src/oneTBB -->

<!-- PMA https://github.com/2600254/Packed-Memory-Array -->

<!-- dynamic_bitset https://github.com/pinam45/dynamic_bitset -->
