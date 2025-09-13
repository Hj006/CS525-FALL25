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
   Contains provided test cases for verifying the implementation of the buffer manager. The test_assign2_1.c covers the FIFO and LRU replacement strategies. The Makefile compiles these files into the test executable.

7. **test_helper.h**  
   A helper header file used by `test_assign2_1.c / test_assign2_2.c` for testing convenience. It provides macros and utility functions to simplify writing and running tests.

8. **README.md**  
   This document. It describes the solution, design, and instructions for building and running the project.



## 2. Design and Implementation of Functions

### 2.1 Core Design

#### Key Data Structures

1. Frame: Represents one page frame in the buffer pool.
   It contains:
   - pageNum: The page number of the disk page stored in this frame.
   - data: A pointer to the page's content in memory.
   - dirty: A boolean flag that indicates if the page has been modified.
   - fixCount: The page's fix count, which tracks how many clients are using this page.
   - ref: A reference field for replacement strategies, for example, a timestamp for LRU.

2. PoolMgmtData: Stores all the management information for the entire buffer pool.
   It contains:
   - frames: A pointer to the array of Frame structures.
   - numReadIO, numWriteIO: Counters for I/O operations.
   - nextVictim: A pointer used for the FIFO strategy.
   - clockHand: The clock hand pointer used for the CLOCK strategy.
   - strategyData: A pointer for extra data needed by a strategy.

#### Page Replacement Strategies

1. FIFO (First-In, First-Out):
This is implemented using a circular queue concept. The `nextVictim` pointer points to the next frame that is a candidate for replacement. When a replacement is needed, we choose the first frame it points to that has a `fixCount` of 0.

2. LRU (Least Recently Used):
We use a global counter `globalLRUCounter` as a timestamp. Whenever a page is accessed via `pinPage`, we update its `ref` field with the current value of this counter. When a replacement is needed, we choose the frame with the smallest `ref` value that also has a `fixCount` of 0.

3. CLOCK:
This strategy is an improvement on FIFO. We use the `clockHand` as the clock pointer. When searching for a victim, we start scanning from the `clockHand`'s position. If a frame's reference bit is 1, we set it to 0 and continue scanning. If the reference bit is 0, that frame is chosen for replacement.

4. LRU-K:
   ******TODOTODOTODOTODO****** 
   
### 2.2 Function Descriptions

#### Pool Handling (init, shutdown, flush) 
1. **initBufferPool** : To create a new buffer pool in memory, which includes a specified number of page frames.
First, it allocates memory for a `PoolMgmtData` struct, which holds all keeping information for the pool.
Next, it allocates an array of `Frame` structs, one for each page frame in the pool. Each frame is initialized with default values: `pageNum` is set to `NO_PAGE`, `dirty` is `false`, `fixCount` is 0, and memory is allocated for the page data itself.
Finally, it sets up the `BM_BufferPool` handle passed by the page file name, number of pages, and chosen replacement strategy.

2. **shutdownBufferPool** : To safely destroy the buffer pool and free resources.
First, it checks if no pages are currently pinned. If any page is still in use, it returns an error.
Next, it writes dirty pages back to the disk file. This is similar to calling `forceFlushPool`.
Finally, it frees all allocated memory, including the data buffer for each frame, the array of frames, and the `PoolMgmtData` struct itself.

3. **forceFlushPool** : To write all dirty pages from the buffer pool to disk. It iterates through all frames and writes the content back to the page file for any frame that is dirty and not pinned.

#### Page Access (pin, unpin, mark, force)
1. **pinPage** : To request a page from the page file and "pin" it in a frame in memory.
First, it checks if the requested page is already present in one of the frames.
- If yes, it increments that frame's `fixCount` and returns a pointer to its data. For LRU, it also updates a timestamp to mark it as recently used.
- If no, it selects a frame to replace. The selection is based on the pool's replacement strategy, FIFO or LRU. The frames can only be replaced when their `fixCount` is 0.
Next, if the chosen frame contains a dirty page, its content will be written to disk before it is replaced.
Then, it reads the requested page from the disk file into the chosen frame's data buffer.
Finally, it updates the frame's metadata: `pageNum` is set to the new page's number, `fixCount` is set to 1, and the `dirty` flag is cleared.

2. **unpinPage** : To notify the buffer manager that the usage is finished with a page. It finds the corresponding frame and decrements its `fixCount` by one.
3. **markDirty** :  To mark a page in the buffer as having been modified. It finds the corresponding frame and sets its `dirty flag` to true. 
4. **forcePage** : To write the current content of a specific page back to the disk.


#### Statistics (get functions)
1. **getFrameContents** : To get an array of page numbers representing the content of the buffer pool. It returns an array where the i-th element is the page number stored in the i-th frame.
2. **getDirtyFlags** : To get an array of booleans. The i-th element is true if the page in the i-th frame is dirty.
3. **getFixCounts** : To get an array of integers. The i-th element is the `fixCount` of the page in the i-th frame.
4. **getNumReadIO** : To get the total number of pages read from disk.
5. **getNumWriteIO** : To get the total number of pages written to disk.


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

### Run Tests:
Builds the project and immediately executes the test cases.
1. Run the main test case (FIFO and LRU):
   ```bash
   make run1
   ```
2. Run the second test case (for optional strategies):
   ```bash
   make run2
   ```
### Additional Targets

1. **Clean build files**

  ```bash
  make clean
  ```

  Removes all `.o` files and the compiled binary.

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



### Step 3: Execute the test programs using the run targets

```bash
# Run the first test
make run1

# Run the second test
make run2
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





