# CS525-F25-G02

## 1. File Introduction

For this assignment, the main modifications were made to the following files:
- **Makefile**  
- **buffer_mgr.c**  
- **README.md**

### Overview of Files in the Project

1. **Makefile**  
   Used to compile the project. It builds the binary `test_assign1_1` from `test_assign1_1.c`, together with `dberror.c`， `storage_mgr.c` and the buffer manager implementation files. It also includes targets for cleaning build files and running the program.

2. **buffer_mgr.c**  
   This is the core implementation file for the buffer manager. It defines the functions declared in `buffer_mgr.h` for initializing a buffer pool, managing pages using replacement strategies, and tracking statistics.

3. **storage_mgr.h**  
   Header file for the buffer manager. It declares the public interface that `buffer_mgr.c` implements, including data structures like `BM_BufferPool` and `BM_PageHandle`.

4. **storage_mgr.c / storage_mgr.h**
   These files from the 1st assignment provide the underlying storage layer. The buffer manager uses these functions to read pages from the disk file and write pages to the disk file.

5. **dberror.c / dberror.h**  
   Utility files for error handling. They define error codes and provide helper functions for printing and debugging error messages.

6. **test_assign2_1.c / test_assign2_2.c**  
   Contains provided test cases for verifying the implementation of the buffer manager. The tests cover the FIFO and LRU replacement strategies, as well as error handling conditions. The Makefile compiles this file into the test executable.

7. **test_helper.h**  
   A helper header file used by `test_assign2_1.c / test_assign2_2.c` for testing convenience. It provides macros and utility functions to simplify writing and running tests.

8. **README.md**  
   This document. It describes the solution, design, and instructions for building and running the project.



## 2. Design and Implementation of Functions

### Pool Handling (init, shutdown, flush) 
1. **initBufferPool** : 



2. **shutdownBufferPool** : 



3. **forceFlushPool** : 


### Page Access (pin, unpin, mark, force)
1. **pinPage** : 


2. **unpinPage** : 
3. **markDirty** :  
4. **forcePage** : 


### Statistics (get functions)
1. **getFrameContents** : 


2. **getDirtyFlags** : 



3. **getFixCounts** :
4. **getNumReadIO** :
5. **getNumWriteIO** :


## 3. How to Build and Run

### Prerequisites
- **Linux / macOS**  
  Requires `gcc` and `make` (these are usually pre-installed or can be installed easily with package managers such as `apt` or `yum`).

- **Windows**  
  In our setup, we use **WSL (Windows Subsystem for Linux)** with Ubuntu as the development environment.  
  This allows us to run `gcc` and `make` just like on Linux.  

  Example of using WSL and running `make` inside WSL:  
  ******TODOTODOTODOTODO****** 

### Build Instructions
1. Open a terminal (Linux/macOS) or a WSL terminal (Windows).
2. Navigate to the project directory:
   ```bash
   cd CS525-F25-G02/assign2
   ```

3. Run the following command to build the executable:

   ```bash
   make
   ```

   This compiles the source files and generates the executable **`test_assign2_1`** and **`test_assign2_2`**.

4. Run the program after building:

   ```bash
   ./test_assign2_1
   ```

### Additional Targets

1. **Clean build files**

  ```bash
  make clean
  ```

  Removes all `.o` files and the compiled binary.

2. **Compile and run in one step**

  ```bash
  make run
  ```

  Builds the project and immediately executes the primary test case. ******TODO Makefile to define which test make run executes TODO******




## 4. Demonstration of Execution

This section demonstrates how to build, run, and clean the project.  
All commands are executed inside the project directory (`/CS525/CS525-F25-G02/assign2`) using WSL.

### Step 1: Navigate to the project folder
```bash
cd CS525/
cd CS525-F25-G02/
cd assign2
```



### Step 2: Build the project with `make`

```bash
make
```



### Step 3: Execute the test program

```bash
./test_assign2_1
```



### Step 4: Clean build files

```bash
make clean
```






## 5. Video Link

  ******TODOTODOTODOTODO****** 

## 6. Contact Authors

* **Hongyi Jiang** (A20506636)
* **Naicheng Wei** (A20278475)

If you have any questions, feel free to contact us at: **[jiangxiaobai1142@gmail.com](mailto:jiangxiaobai1142@gmail.com)** **[lwei3@ghawk.illinoistech.edu](mailto:lwei3@ghawk.illinoistech.edu)**


