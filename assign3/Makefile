# ============================================================
# Makefile for Record Manager Assignment (assign3)
# ============================================================

CC = gcc
CFLAGS = -Wall -g

# Source files
SRCS = dberror.c storage_mgr.c buffer_mgr.c record_mgr.c expr.c rm_serializer.c test_assign3_1.c

OBJS = $(SRCS:.c=.o)

# Output executable
TARGET = test_assign3_1

# Default target: build everything
all: $(TARGET)

# Link all object files into the final executable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

# Compile each .c file into a .o file
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up build artifacts
clean:
	rm -f $(OBJS) $(TARGET)

# Run the compiled test program
run: $(TARGET)
	./$(TARGET)

# ============================================================
# Usage:
#   make         -> compile the project
#   make run     -> compile and run the test program
#   make clean   -> remove object files and the executable
# ============================================================
