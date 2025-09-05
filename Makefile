# Compiler
CC = gcc
CFLAGS = -Wall -g

# Target executable name
TARGET = test_assign1_1

# Source files
SRCS = dberror.c storage_mgr.c test_assign1_1.c
OBJS = $(SRCS:.c=.o)

# Default target: build the executable
all: $(TARGET)

# Link object files to create the final executable
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS)

# Compilation rule: convert .c files to .o files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up generated files
clean:
	rm -f $(OBJS) $(TARGET)

# Convenience target: compile and run the program
run: all
	./$(TARGET)
