# Compiler
CC = gcc
CFLAGS = -Wall -g

# Targets
TARGETS = test_assign2_1 test_assign2_2

# Common source files
COMMON_SRCS = buffer_mgr.c buffer_mgr_stat.c dberror.c storage_mgr.c
COMMON_OBJS = $(COMMON_SRCS:.c=.o)

# Test 1
TEST1 = test_assign2_1
TEST1_SRCS = test_assign2_1.c
TEST1_OBJS = $(TEST1_SRCS:.c=.o)

# Test 2
TEST2 = test_assign2_2
TEST2_SRCS = test_assign2_2.c
TEST2_OBJS = $(TEST2_SRCS:.c=.o)

# Default target: build everything
all: $(TARGETS)

# Build test 1
$(TEST1): $(COMMON_OBJS) $(TEST1_OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Build test 2
$(TEST2): $(COMMON_OBJS) $(TEST2_OBJS)
	$(CC) $(CFLAGS) -o $@ $^

# Compilation rule
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up
clean:
	rm -f $(COMMON_OBJS) $(TEST1_OBJS) $(TEST2_OBJS) $(TARGETS)

# Run tests
run1: $(TEST1)
	./$(TEST1)

run2: $(TEST2)
	./$(TEST2)
