# ============================================================
# Makefile for Record Manager Assignment (assign3)
# ============================================================

CC = gcc
CFLAGS = -Wall -g

# Common source files
COMMON_SRCS = dberror.c storage_mgr.c buffer_mgr.c expr.c rm_serializer.c
COMMON_OBJS = $(COMMON_SRCS:.c=.o)

# ============================================================
# Test 1: Record Manager Test
# ============================================================

RM_SRCS = record_mgr.c test_assign3_1.c
RM_OBJS = $(RM_SRCS:.c=.o)
RM_TARGET = test_assign3_1

# ============================================================
# Test 2: Expression Test
# ============================================================

EXPR_SRCS = expr.c test_expr.c dberror.c rm_serializer.c record_mgr.c storage_mgr.c buffer_mgr.c
EXPR_OBJS = $(EXPR_SRCS:.c=.o)
EXPR_TARGET = test_expr

# ============================================================
# Default: build both tests
# ============================================================

all: $(RM_TARGET) $(EXPR_TARGET)

# ------------------------------------------------------------
# Record Manager executable
# ------------------------------------------------------------
$(RM_TARGET): $(COMMON_OBJS) $(RM_OBJS)
	$(CC) $(CFLAGS) -o $(RM_TARGET) $(COMMON_OBJS) $(RM_OBJS)

# ------------------------------------------------------------
# Expression Test executable
# ------------------------------------------------------------
$(EXPR_TARGET): $(EXPR_OBJS)
	$(CC) $(CFLAGS) -o $(EXPR_TARGET) $(EXPR_OBJS)

# ------------------------------------------------------------
# Generic compilation rule
# ------------------------------------------------------------
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# ------------------------------------------------------------
# Run tests
# ------------------------------------------------------------
run: $(RM_TARGET)
	./$(RM_TARGET)

run_expr: $(EXPR_TARGET)
	./$(EXPR_TARGET)

# ------------------------------------------------------------
# Clean up
# ------------------------------------------------------------
clean:
	rm -f *.o $(RM_TARGET) $(EXPR_TARGET)

# ============================================================
# Usage:
#   make             -> build all tests
#   make run         -> run record manager tests
#   make run_expr    -> run expression tests
#   make clean       -> remove object files and executables
# ============================================================
