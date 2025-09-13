# CS525-F25-G02

## 1. File Introduction

For this assignment, the main modifications were made to the following files:
- **Makefile**  
- **buffer_mgr.c**  
- **README.md**

### Overview of Files in the Project

1. **Makefile**  
   Used to compile the project. It builds the binary `test_assign1_1` from `test_assign1_1.c`, together with `dberror.c` and the storage manager implementation files. It also includes targets for cleaning build files and running the program.

2. **storage_mgr.c**  
   This is the core implementation file for the storage manager. It defines the functions declared in `storage_mgr.h`, including creating, opening, reading, writing, and destroying page files. It also implements helper functions for ensuring file capacity and appending new pages.

3. **storage_mgr.h**  
   Header file for the storage manager. It declares the public interface that `storage_mgr.c` implements.

4. **dberror.c / dberror.h**  
   Utility files for error handling. They define error codes and provide helper functions for printing and debugging error messages.

5. **test_assign1_1.c**  
   Contains provided test cases for verifying the implementation of the storage manager. The Makefile compiles this file into the test executable.

6. **test_helper.h**  
   A helper header file used by `test_assign1_1.c` for testing convenience. It provides macros and utility functions to simplify writing and running tests.

7. **README.md**  
   This document. It describes the solution, design, and instructions for building and running the project.



## 2. Design and Implementation of Functions

### 
1. **createPageFile** : 



2. **openPageFile** : 



3. **closePageFile** : 
4. **destroyPageFile** : 

### 
1. **readBlock** : 


2. **readFirstBlock** : 
3. **readPreviousBlock** :  
4. **readNextBlock** : 
5. **readLastBlock** : 

### 
1. **writeBlock** : 


2. **appendEmptyBlock** : 



3. **ensureCapacity** : 


## 3. How to Build and Run

### Prerequisites
- **Linux / macOS**  
  Requires `gcc` and `make` (these are usually pre-installed or can be installed easily with package managers such as `apt` or `yum`).

- **Windows**  
  In our setup, we use **WSL (Windows Subsystem for Linux)** with Ubuntu as the development environment.  
  This allows us to run `gcc` and `make` just like on Linux.  

  Example of using WSL and running `make` inside WSL:  
  ![Make Build Example](images/make_build.png)  

### Build Instructions
1. Open a terminal (Linux/macOS) or a WSL terminal (Windows).
2. Navigate to the project directory:
   ```bash
   cd CS525-F25-G02
   ```

3. Run the following command to build the executable:

   ```bash
   make
   ```

   This compiles the source files (`dberror.c`, `storage_mgr.c`, `test_assign1_1.c`) and generates the executable **`test_assign1_1`**.

4. Run the program after building:

   ```bash
   ./test_assign1_1
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

  Builds the project and immediately executes `./test_assign1_1`.




## 4. Demonstration of Execution

This section demonstrates how to build, run, and clean the project.  
All commands are executed inside the project directory (`/CS525/CS525-F25-G02`) using WSL.

### Step 1: Navigate to the project folder
```bash
cd CS525/
cd CS525-F25-G02/
```



### Step 2: Build the project with `make`

```bash
make
```



### Step 3: Execute the test program

```bash
./test_assign1_1
```



### Step 4: Clean build files

```bash
make clean
```






## 5. Video Link



## 6. Contact Authors

* **Hongyi Jiang** (A20506636)
* **Naicheng Wei** (A20278475)

If you have any questions, feel free to contact us at: **[jiangxiaobai1142@gmail.com](mailto:jiangxiaobai1142@gmail.com)** **[lwei3@ghawk.illinoistech.edu](mailto:lwei3@ghawk.illinoistech.edu)**

